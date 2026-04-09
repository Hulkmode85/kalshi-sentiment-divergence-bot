"""
Kalshi Sentiment Divergence Bot
Compares Kalshi market prices vs aggregated real-time news sentiment.
When sentiment strongly disagrees with market price, trade the gap.
Free data: RSS feeds + keyword scoring (no paid API needed).
"""

import asyncio
import os
from flask import Flask, jsonify
import threading
import json
import time
import logging
import base64
import re
import uuid
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from typing import Optional
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, ec
import httpx
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sentiment_div")


def _normalize_market(m: dict) -> dict:
    """Normalize Kalshi API v2 dollar-denominated fields to legacy field names."""
    if "yes_bid_dollars" in m and "yes_bid" not in m:
        m["yes_bid"] = m.get("yes_bid_dollars")
        m["yes_ask"] = m.get("yes_ask_dollars")
        m["no_bid"] = m.get("no_bid_dollars")
        m["no_ask"] = m.get("no_ask_dollars")
        m["last_price"] = m.get("last_price_dollars")
        m["volume"] = m.get("volume_fp") or m.get("volume_24h_fp") or m.get("volume", 0)
        m["open_interest"] = m.get("open_interest_fp") or m.get("open_interest", 0)
    for k in ["yes_bid", "yes_ask", "no_bid", "no_ask", "last_price"]:
        v = m.get(k)
        if isinstance(v, str):
            try: m[k] = float(v)
            except: pass
    return m


# ── CONFIG ────────────────────────────────────────────────────────────────────
KALSHI_BASE       = os.getenv("KALSHI_BASE", "https://api.elections.kalshi.com")
KALSHI_API_URL    = os.getenv("KALSHI_API_URL", f"{KALSHI_BASE}/trade-api/v2")
KALSHI_API_KEY    = os.getenv("KALSHI_API_KEY", "")
KALSHI_KEY_ID     = os.getenv("KALSHI_KEY_ID", "")
PAPER_MODE        = os.getenv("PAPER_MODE", "true").lower() == "true"
PAPER_BALANCE     = float(os.getenv("PAPER_BALANCE", "5000"))
BET_SIZE_USD      = float(os.getenv("BET_SIZE_USD", "12"))
MAX_BET_USD       = float(os.getenv("MAX_BET_USD", "35"))
MIN_DIVERGENCE    = float(os.getenv("MIN_DIVERGENCE", "0.10"))  # sentiment vs price must diverge by 10+ pts
MIN_SENTIMENT_CONF= float(os.getenv("MIN_SENTIMENT_CONF", "0.40"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "300"))
LOOKBACK_HOURS    = int(os.getenv("LOOKBACK_HOURS", "12"))

# ── RSS SOURCES ────────────────────────────────────────────────────────────────
RSS_FEEDS = [
    "https://feeds.apnews.com/rss/APNewsTopHeadlines",
    "https://rss.nytimes.com/services/xml/rss/nyt/World.xml",
    "https://feeds.bbci.co.uk/news/rss.xml",
    "https://feeds.bbci.co.uk/news/world/rss.xml",
    "https://feeds.bbci.co.uk/news/business/rss.xml",
    "https://rss.politico.com/politics-news.rss",
    "https://feeds.npr.org/1001/rss.xml",
    "https://feeds.npr.org/1006/rss.xml",
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=10000664",
    "https://www.marketwatch.com/rss/topstories",
    "https://feeds.marketwatch.com/marketwatch/marketpulse/",
]

# ── TOPIC → KALSHI SERIES + SENTIMENT POLARITY ───────────────────────────────
# Each topic maps: keywords → (series list, positive_means_YES_series)
TOPICS = {
    "us_economy": {
        "positive_kw": ["growth","expansion","recovery","hiring","employment","beat expectations",
                        "stronger","resilient","rally","surge","soared","jumped","gained","rose",
                        "added jobs","unemployment fell","gdp beat","retail sales"],
        "negative_kw": ["recession","contraction","layoffs","job cuts","unemployment rose",
                        "slowed","declined","missed","plunged","collapsed","fell sharply",
                        "slowdown","downturn","stagflation","gdp miss","weak jobs"],
        "series": ["KXSPY","KXQQQ","KXGDP","KXRECESSION"],
        "positive_is_yes": {"KXSPY":True,"KXQQQ":True,"KXGDP":True,"KXRECESSION":False},
    },
    "inflation": {
        "positive_kw": ["inflation","cpi","prices rose","costs rise","price surge","hot inflation",
                        "above forecast","price pressure","hotter","higher than expected",
                        "accelerated","increased","tariffs","cost of living"],
        "negative_kw": ["disinflation","deflation","prices fell","inflation cooled","cpi lower",
                        "prices declined","below forecast","cooler","slowing inflation",
                        "price decline","deflationary","lower than expected"],
        "series": ["KXCPI","KXCPIM","KXINFL"],
        "positive_is_yes": {"KXCPI":True,"KXCPIM":True,"KXINFL":True},
    },
    "fed_rate": {
        "positive_kw": ["hike","raise rates","hawkish","tighten","rate increase","hold rates",
                        "no cut","higher for longer","restrictive","pause","hold steady"],
        "negative_kw": ["cut","lower rates","dovish","easing","pivot","rate reduction",
                        "emergency cut","rate decrease","stimulus","accommodation"],
        "series": ["KXFED","KXFEDRATE"],
        "positive_is_yes": {"KXFED":False,"KXFEDRATE":False},  # YES = cut happened
    },
    "crypto": {
        "positive_kw": ["bitcoin","btc","crypto","rally","surge","soared","gains","higher",
                        "adoption","etf","institutional","bullish","all-time","approved"],
        "negative_kw": ["bitcoin crash","crypto crash","btc fell","bankrupt","hack","exploit",
                        "ban","crackdown","regulation","sell-off","plunged","collapsed"],
        "series": ["KXBTC","KXETH","KXCRYPTO"],
        "positive_is_yes": {"KXBTC":True,"KXETH":True,"KXCRYPTO":True},
    },
    "geopolitics": {
        "positive_kw": ["ceasefire","peace","deal","agreement","de-escalation","withdrawal",
                        "talks","diplomacy","sanctions relief","stability","resolved"],
        "negative_kw": ["attack","strike","invasion","escalation","conflict","war","troops",
                        "sanctions","missile","bomb","threat","crisis","tension"],
        "series": ["KXOIL","KXSPY","KXGOLD"],
        "positive_is_yes": {"KXOIL":False,"KXSPY":True,"KXGOLD":False},
    },
    "tariffs_trade": {
        "positive_kw": ["tariff","trade war","import tax","duties","sanctions","trade dispute",
                        "protectionism","trade deficit","restrict","ban imports"],
        "negative_kw": ["trade deal","trade agreement","tariff relief","free trade","reduction",
                        "exempt","waiver","eased tariffs","trade progress"],
        "series": ["KXSPY","KXQQQ","KXCPI"],
        "positive_is_yes": {"KXSPY":False,"KXQQQ":False,"KXCPI":True},
    },
}

# ── AUTH ──────────────────────────────────────────────────────────────────────
def _sign_request(method, path, ts, body=""):
    if not KALSHI_API_KEY:
        return ""
    try:
        pem_str = os.getenv("KALSHI_PRIVATE_KEY", "")
        if "\\n" in pem_str:
            pem_str = pem_str.replace("\\n", "\n")
        private_key = serialization.load_pem_private_key(pem_str.encode(), password=None)
        msg = f"{ts}{method.upper()}{path}{body}".encode()
        if isinstance(private_key, ec.EllipticCurvePrivateKey):
            sig = private_key.sign(msg, ec.ECDSA(hashes.SHA256()))
        else:
            sig = private_key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=32), hashes.SHA256())
        return base64.b64encode(sig).decode()
    except Exception:
        return ""

def _auth_headers(method, path, body=""):
    ts = int(time.time() * 1000)
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": str(ts),
        "KALSHI-ACCESS-SIGNATURE": _sign_request(method, path, ts, body),
    }

# ── PAPER LEDGER ──────────────────────────────────────────────────────────────
@dataclass
class PaperLedger:
    balance: float = PAPER_BALANCE
    trades: list = field(default_factory=list)
    wins: int = 0
    losses: int = 0

    def record(self, market, side, contracts, price_cents, signal):
        cost = contracts * price_cents / 100
        self.balance -= cost
        self.trades.append({"ts": datetime.now(timezone.utc).isoformat(),
            "market": market, "side": side, "contracts": contracts,
            "price_cents": price_cents, "cost": cost, "signal": signal})
        log.info(f"[PAPER] {side} {contracts}ct @ {price_cents}¢ on {market} | {signal} | bal=${self.balance:.2f}")

# ── RSS FETCHING ──────────────────────────────────────────────────────────────
async def fetch_rss_items(client: httpx.AsyncClient, url: str) -> list[dict]:
    try:
        r = await client.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code != 200:
            return []
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        items = []
        for block in re.findall(r'<item>(.*?)</item>', r.text, re.DOTALL)[:25]:
            title = re.search(r'<title[^>]*>(.*?)</title>', block, re.DOTALL)
            desc  = re.search(r'<description[^>]*>(.*?)</description>', block, re.DOTALL)
            pub   = re.search(r'<pubDate>(.*?)</pubDate>', block, re.DOTALL)
            t = re.sub(r'<[^>]+>','', title.group(1) if title else '').strip()
            d = re.sub(r'<[^>]+>','', desc.group(1)  if desc  else '').strip()[:200]
            pub_dt = datetime.now(timezone.utc)
            if pub:
                try:
                    from email.utils import parsedate_to_datetime
                    pub_dt = parsedate_to_datetime(pub.group(1).strip())
                    if not pub_dt.tzinfo:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                except Exception:
                    pass
            if pub_dt >= cutoff and t:
                items.append({"title": t, "desc": d, "published": pub_dt})
        return items
    except Exception as e:
        log.debug(f"RSS error {url}: {e}")
        return []

# ── SENTIMENT SCORING ─────────────────────────────────────────────────────────
@dataclass
class TopicSentiment:
    topic: str
    score: float        # -1.0 (very negative) to +1.0 (very positive)
    confidence: float   # 0-1
    article_count: int
    sample_title: str

def score_articles(articles: list[dict]) -> dict[str, TopicSentiment]:
    """Score each article against topic keywords, aggregate by topic."""
    results = {}
    for topic_name, topic in TOPICS.items():
        pos_hits = 0
        neg_hits = 0
        total_articles = 0
        sample = ""
        for art in articles:
            text = (art["title"] + " " + art["desc"]).lower()
            p = sum(1 for kw in topic["positive_kw"] if kw in text)
            n = sum(1 for kw in topic["negative_kw"] if kw in text)
            if p > 0 or n > 0:
                total_articles += 1
                pos_hits += p
                neg_hits += n
                if not sample:
                    sample = art["title"][:60]

        if total_articles == 0:
            continue

        total_hits = pos_hits + neg_hits
        score = (pos_hits - neg_hits) / max(total_hits, 1)
        confidence = min(0.50 + total_articles * 0.05 + total_hits * 0.02, 0.85)
        results[topic_name] = TopicSentiment(
            topic=topic_name, score=score, confidence=confidence,
            article_count=total_articles, sample_title=sample
        )
        log.info(f"[SENTIMENT] {topic_name}: score={score:+.2f} conf={confidence:.2f} n={total_articles} | {sample[:50]}")

    return results

# ── KALSHI MARKET FETCH ───────────────────────────────────────────────────────
async def get_kalshi_markets(client: httpx.AsyncClient, series: str) -> list:
    path = f"/markets?series_ticker={series}&status=open&limit=20"
    headers = _auth_headers("GET", path) if KALSHI_KEY_ID else {"Content-Type": "application/json"}
    try:
        r = await client.get(f"{KALSHI_API_URL}{path}", headers=headers, timeout=10)
        return r.json().get("markets", []) if r.status_code == 200 else []
    except Exception:
        return []

# ── DIVERGENCE DETECTION ──────────────────────────────────────────────────────
def find_divergence_trade(markets: list, sentiment: TopicSentiment,
                          topic: dict) -> Optional[dict]:
    """
    Find markets where Kalshi price diverges from sentiment.
    If sentiment is bullish (+) but YES price is low → buy YES (market underpriced).
    If sentiment is bearish (-) but YES price is high → buy NO (market overpriced).
    """
    best = None
    best_divergence = 0.0

    for m in markets:
        _normalize_market(m)
        series = m.get("series_ticker", "")
        yes_ask = m.get("yes_ask", 0)
        no_ask  = m.get("no_ask", 0)
        if not yes_ask or not no_ask:
            continue

        # Time filter
        close_ts = m.get("close_time") or m.get("expiration_time") or ""
        if close_ts:
            try:
                close_dt = datetime.fromisoformat(close_ts.replace("Z", "+00:00"))
                if (close_dt - datetime.now(timezone.utc)).total_seconds() < 3600:
                    continue
            except Exception:
                pass

        positive_is_yes = topic["positive_is_yes"].get(series, True)
        market_yes_prob = yes_ask / 100

        if positive_is_yes:
            # Positive sentiment → should push YES up
            # Sentiment score (+1 = fully positive) → implied true_prob
            sentiment_implied_prob = 0.50 + sentiment.score * 0.40  # maps [-1,1] → [0.10, 0.90]
            divergence = sentiment_implied_prob - market_yes_prob

            if divergence >= MIN_DIVERGENCE and sentiment.confidence >= MIN_SENTIMENT_CONF:
                # Market underprices YES relative to sentiment → buy YES
                side, price = "yes", yes_ask
                edge = divergence * sentiment.confidence
                if edge > best_divergence:
                    best_divergence = edge
                    best = {"market": m, "side": side, "price": price,
                            "divergence": divergence, "edge": edge,
                            "note": f"{sentiment.topic} sent={sentiment.score:+.2f} mkt={market_yes_prob:.2f} → BUY YES"}

            elif -divergence >= MIN_DIVERGENCE and sentiment.confidence >= MIN_SENTIMENT_CONF:
                # Market overprices YES → buy NO
                side, price = "no", no_ask
                edge = (-divergence) * sentiment.confidence
                if edge > best_divergence:
                    best_divergence = edge
                    best = {"market": m, "side": side, "price": price,
                            "divergence": -divergence, "edge": edge,
                            "note": f"{sentiment.topic} sent={sentiment.score:+.2f} mkt={market_yes_prob:.2f} → BUY NO"}

        else:
            # Negative sentiment → should push YES down (bearish)
            sentiment_implied_prob = 0.50 - sentiment.score * 0.40
            divergence = sentiment_implied_prob - market_yes_prob

            if divergence >= MIN_DIVERGENCE and sentiment.confidence >= MIN_SENTIMENT_CONF:
                side, price = "yes", yes_ask
                edge = divergence * sentiment.confidence
                if edge > best_divergence:
                    best_divergence = edge
                    best = {"market": m, "side": side, "price": price,
                            "divergence": divergence, "edge": edge,
                            "note": f"{sentiment.topic} inverted sent={sentiment.score:+.2f} mkt={market_yes_prob:.2f} → BUY YES"}

            elif -divergence >= MIN_DIVERGENCE and sentiment.confidence >= MIN_SENTIMENT_CONF:
                side, price = "no", no_ask
                edge = (-divergence) * sentiment.confidence
                if edge > best_divergence:
                    best_divergence = edge
                    best = {"market": m, "side": side, "price": price,
                            "divergence": -divergence, "edge": edge,
                            "note": f"{sentiment.topic} inverted sent={sentiment.score:+.2f} mkt={market_yes_prob:.2f} → BUY NO"}

    return best

# ── ORDER EXECUTION ───────────────────────────────────────────────────────────
async def place_order(client, ticker, side, price_cents, contracts, ledger, note):
    if PAPER_MODE:
        ledger.record(ticker, side, contracts, price_cents, note)
        return True
    body = json.dumps({"ticker": ticker, "action": "buy", "side": side,
                       "type": "limit", "count": contracts,
                       "yes_price" if side == "yes" else "no_price": price_cents,
                       "client_order_id": str(uuid.uuid4())})
    path = "/portfolio/orders"
    try:
        r = await client.post(f"{KALSHI_API_URL}{path}", headers=_auth_headers("POST", path, body),
                              content=body, timeout=10)
        return r.status_code in (200, 201)
    except Exception:
        return False

# ── COOLDOWN ──────────────────────────────────────────────────────────────────
class CooldownTracker:
    def __init__(self, minutes=90):
        self.minutes = minutes
        self._last = {}

    def can_trade(self, key):
        if key not in self._last:
            return True
        return (datetime.now(timezone.utc) - self._last[key]).total_seconds() > self.minutes * 60

    def mark(self, key):
        self._last[key] = datetime.now(timezone.utc)

# ── MAIN ──────────────────────────────────────────────────────────────────────
# ── Stats HTTP server ─────────────────────────────────────────────────────────
_stats_app = Flask(__name__)
_bot_stats = {"trades": 0, "wins": 0, "pnl": 0.0, "balance": 0.0, "start": time.time()}

@_stats_app.route("/stats")
def _stats_endpoint():
    t = _bot_stats
    total = t["trades"]
    return jsonify({"bot": "kalshi-sentiment-divergence-bot", "paper_mode": True,
        "balance": t["balance"], "trades": total, "wins": t["wins"],
        "losses": total - t["wins"], "win_rate": round(t["wins"]/max(total,1), 4),
        "pnl": t["pnl"], "uptime_hours": round((time.time()-t["start"])/3600, 2)})

@_stats_app.route("/health")
def _health_endpoint():
    return jsonify({"status": "ok"})

def _run_stats_server():
    _stats_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


async def main():
    log.info(f"=== Kalshi Sentiment Divergence Bot (paper={PAPER_MODE}) ===")
    log.info(f"MIN_DIVERGENCE={MIN_DIVERGENCE*100:.0f}pts, MIN_CONF={MIN_SENTIMENT_CONF}, poll={POLL_INTERVAL_SEC}s")

    paper    = PaperLedger()
    _bot_stats['balance'] = paper.balance
    threading.Thread(target=_run_stats_server, daemon=True).start()
    cooldown = CooldownTracker(minutes=90)
    trades   = 0

    async with httpx.AsyncClient() as client:
        while True:
            _bot_stats["balance"] = paper.balance
            _bot_stats["trades"] = len(paper.trades)
            _bot_stats["wins"] = paper.wins
            log.info(f"--- Scan | bal=${paper.balance:.2f} | trades={trades} ---")

            # 1. Fetch all RSS
            all_articles = []
            for feed in RSS_FEEDS:
                items = await fetch_rss_items(client, feed)
                all_articles.extend(items)
                await asyncio.sleep(0.4)
            log.info(f"Fetched {len(all_articles)} articles")

            # 2. Score sentiment by topic
            sentiments = score_articles(all_articles)

            # 3. For each topic with strong sentiment, find divergent markets
            for topic_name, sentiment in sentiments.items():
                if abs(sentiment.score) < 0.25:
                    continue  # No clear signal
                if sentiment.confidence < MIN_SENTIMENT_CONF:
                    continue

                cd_key = f"{topic_name}_{'+' if sentiment.score > 0 else '-'}"
                if not cooldown.can_trade(cd_key):
                    continue

                topic = TOPICS[topic_name]
                all_markets = []
                for series in topic["series"]:
                    mkts = await get_kalshi_markets(client, series)
                    all_markets.extend(mkts)
                    await asyncio.sleep(0.3)

                if not all_markets:
                    continue

                trade = find_divergence_trade(all_markets, sentiment, topic)
                if not trade:
                    log.info(f"{topic_name}: no divergence trade found in {len(all_markets)} markets")
                    continue

                price     = trade["price"]
                contracts = max(1, min(int(BET_SIZE_USD * 100 / price), int(MAX_BET_USD * 100 / price)))
                ticker    = trade["market"].get("ticker", "?")

                log.info(f"[TRADE] {ticker} | {trade['side'].upper()} {contracts}ct @ {price}¢ | "
                         f"div={trade['divergence']*100:.1f}pts edge={trade['edge']*100:.1f}% | {trade['note']}")

                if await place_order(client, ticker, trade["side"], price, contracts, paper, trade["note"]):
                    cooldown.mark(cd_key)
                    trades += 1

                await asyncio.sleep(1.0)

            log.info(f"--- Complete | sleeping {POLL_INTERVAL_SEC}s ---")
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    asyncio.run(main())
