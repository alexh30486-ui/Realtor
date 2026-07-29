from __future__ import annotations

import argparse
import asyncio
import enum
import logging
import os
import re
import smtplib
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional, Type, Union
from urllib.parse import urljoin, urlparse

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("realtor_pro_system.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("realtor-pro-actor-system")


@dataclass
class Config:
    # Scraper configuration
    scraper_backend: str = os.getenv("SCRAPER_BACKEND", "mock").lower()

    # Target source. {zip} is substituted per ZIP code at scrape time.
    # The old hardcoded "https://www.example.com/homes/fsbo/{zip}_rb/" only
    # worked as a demo placeholder - point this at a real, permitted source.
    source_url_template: str = os.getenv(
        "FSBO_SOURCE_URL_TEMPLATE", "https://www.example.com/homes/fsbo/{zip}_rb/"
    )

    # CSS Selectors for HttpScraperBackend (configurable via environment)
    card_selector: str = os.getenv("FSBO_LISTING_CARD_SELECTOR", ".listing-card, .property-card, article")
    address_selector: str = os.getenv("FSBO_ADDRESS_SELECTOR", ".address, [data-test='property-card-addr'], address")
    price_selector: str = os.getenv("FSBO_PRICE_SELECTOR", ".price, [data-test='property-card-price']")
    link_selector: str = os.getenv("FSBO_LINK_SELECTOR", "a.property-card-link, a[href*='/homedetails/'], a")
    beds_selector: str = os.getenv("FSBO_BEDS_SELECTOR", ".beds, [aria-label*='bed']")
    baths_selector: str = os.getenv("FSBO_BATHS_SELECTOR", ".baths, [aria-label*='bath']")
    sqft_selector: str = os.getenv("FSBO_SQFT_SELECTOR", ".sqft, [aria-label*='sqft']")

    # Integrations
    supabase_url: str = os.getenv("SUPABASE_URL", "")
    supabase_key: str = os.getenv("SUPABASE_KEY", "")

    skip_trace_api_key: str = os.getenv("SKIP_TRACE_API_KEY", "")
    skip_trace_provider: str = os.getenv("SKIP_TRACE_PROVIDER", "mock")

    gmail_user: str = os.getenv("GMAIL_USER", "")
    gmail_app_password: str = os.getenv("GMAIL_APP_PASSWORD", "")

    langfuse_public_key: str = os.getenv("LANGFUSE_PUBLIC_KEY", "")
    langfuse_secret_key: str = os.getenv("LANGFUSE_SECRET_KEY", "")
    langfuse_host: str = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")

    max_items_per_zip: int = int(os.getenv("MAX_ITEMS_PER_ZIP", "25"))

    # --- Skip trace hardening: retries + cost guard + rate limit ---
    skip_trace_max_retries: int = int(os.getenv("SKIP_TRACE_MAX_RETRIES", "3"))
    skip_trace_backoff_base_secs: float = float(os.getenv("SKIP_TRACE_BACKOFF_BASE_SECS", "1.0"))
    skip_trace_max_calls_per_run: int = int(os.getenv("SKIP_TRACE_MAX_CALLS_PER_RUN", "200"))
    skip_trace_min_interval_secs: float = float(os.getenv("SKIP_TRACE_MIN_INTERVAL_SECS", "0.25"))

    # --- Dedup / status machine ---
    dedup_state_file: str = os.getenv("DEDUP_STATE_FILE", "dedup_state.json")

    # --- Scheduler ---
    rescrape_cooldown_hours: float = float(os.getenv("RESCRAPE_COOLDOWN_HOURS", "24"))
    scrape_state_file: str = os.getenv("SCRAPE_STATE_FILE", "scrape_state.json")

    # --- Observability ---
    log_format: str = os.getenv("LOG_FORMAT", "text").lower()  # text | json
    notify_webhook_url: str = os.getenv("NOTIFY_WEBHOOK_URL", "")

    # --- Runtime flags (set from CLI, not env) ---
    dry_run: bool = False

    def missing_required(self) -> List[str]:
        required = {
            "SUPABASE_URL": self.supabase_url,
            "SUPABASE_KEY": self.supabase_key,
        }
        missing = [name for name, val in required.items() if not val]
        if self.scraper_backend != "mock" and "example.com" in self.source_url_template:
            missing.append("FSBO_SOURCE_URL_TEMPLATE (still pointing at the placeholder domain)")
        return missing


@dataclass
class Listing:
    zip_code: str
    address: str
    price: Optional[float] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    sqft: Optional[float] = None
    listing_url: Optional[str] = None
    source: str = "fsbo_scraper"

    owner_name: Optional[str] = None
    owner_phone: Optional[str] = None
    owner_email: Optional[str] = None
    skip_trace_status: str = "pending"

    outreach_status: str = "not_started"
    lead_status: str = "new"  # new -> traced -> contacted -> responded

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class MessagePriority(enum.Enum):
    LOW = 1
    NORMAL = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class ActorMessage:
    sender_id: str
    recipient_id: str
    action: str
    payload: Dict[str, Any] = field(default_factory=dict)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    correlation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: float = field(default_factory=time.time)
    priority: MessagePriority = MessagePriority.NORMAL


class Tracer:
    """Wraps Langfuse spans around pipeline stages. Degrades to a no-op if
    LANGFUSE_PUBLIC_KEY/LANGFUSE_SECRET_KEY are unset or init fails - those
    fields were already in Config but nothing consumed them before this."""

    def __init__(self, cfg: Config):
        self.enabled = bool(cfg.langfuse_public_key and cfg.langfuse_secret_key)
        self._client = None
        if self.enabled:
            try:
                from langfuse import Langfuse

                self._client = Langfuse(
                    public_key=cfg.langfuse_public_key,
                    secret_key=cfg.langfuse_secret_key,
                    host=cfg.langfuse_host,
                )
                logger.info("Langfuse configured")
            except Exception as e:  # pragma: no cover
                logger.warning(f"Langfuse init failed, tracing disabled: {e}")
                self.enabled = False

    def span(self, name: str, **metadata):
        return _SpanCtx(name, metadata)


class _SpanCtx:
    def __init__(self, name: str, metadata: dict):
        self.name = name
        self.metadata = metadata
        self._t0 = None

    def __enter__(self):
        self._t0 = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        dur = time.time() - self._t0
        status = "error" if exc else "ok"
        logger.debug(f"[trace] {self.name} ({status}, {dur:.2f}s) {self.metadata}")
        return False


class _NullSpan:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        import json as _json

        payload = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return _json.dumps(payload)


def configure_log_format(cfg: "Config") -> None:
    """Swaps the root handlers' formatter to JSON if LOG_FORMAT=json. Called
    once at startup; safe no-op for the default 'text' format."""
    if cfg.log_format != "json":
        return
    fmt = _JsonLogFormatter()
    for handler in logging.getLogger().handlers:
        handler.setFormatter(fmt)


class SeenStore:
    """
    Local, file-backed record of every address we've ever processed, keyed
    by address. Enables:
      - dedup: don't re-contact someone we already messaged
      - cache reuse: don't re-pay for a skip trace we already have
      - a simple new -> traced -> contacted -> responded status machine

    Deliberately has zero external dependencies (plain JSON file) so dedup
    works even with no Supabase/DB configured at all. If you *do* have
    Supabase configured, StoreActor is still the system of record - this is
    a fast local cache layered in front of it.
    """

    def __init__(self, path: str):
        self.path = path
        self._data: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        import json as _json

        if os.path.exists(self.path):
            try:
                with open(self.path, "r") as f:
                    self._data = _json.load(f)
            except Exception as e:
                logger.warning(f"[SeenStore] Failed to load {self.path}, starting fresh: {e}")
                self._data = {}

    def save(self) -> None:
        import json as _json

        try:
            tmp_path = f"{self.path}.tmp"
            with open(tmp_path, "w") as f:
                _json.dump(self._data, f, indent=2, default=str)
            os.replace(tmp_path, self.path)
        except Exception as e:
            logger.error(f"[SeenStore] Failed to persist {self.path}: {e}")

    def get(self, address: str) -> Optional[Dict[str, Any]]:
        return self._data.get(address)

    def upsert(self, address: str, **fields: Any) -> None:
        record = self._data.setdefault(address, {})
        record.update(fields)
        record["last_updated"] = time.time()

    def is_contacted(self, address: str) -> bool:
        rec = self._data.get(address)
        return bool(rec and rec.get("lead_status") == "contacted")

    def cached_owner(self, address: str) -> Optional[Dict[str, Any]]:
        rec = self._data.get(address)
        if rec and rec.get("lead_status") in ("traced", "contacted"):
            return rec
        return None


class ScraperParsingUtils:
    @staticmethod
    def parse_price(price_str: Optional[str]) -> Optional[float]:
        """Extracts numeric price from formatted strings e.g. '$489,000' -> 489000.0."""
        if not price_str:
            return None
        cleaned = re.sub(r"[^\d.]", "", price_str)
        try:
            return float(cleaned) if cleaned else None
        except ValueError:
            return None

    @staticmethod
    def parse_number(text_str: Optional[str]) -> Optional[float]:
        """Extracts first numeric scalar from strings like '3 bd', '2.5 baths', '1,850 sqft'."""
        if not text_str:
            return None
        match = re.search(r"(\d+(?:\.\d+)?)", text_str.replace(",", ""))
        if match:
            try:
                return float(match.group(1))
            except ValueError:
                return None
        return None

    @staticmethod
    def resolve_url(base_url: str, relative_path: Optional[str]) -> Optional[str]:
        """Resolves relative links to clean absolute URLs."""
        if not relative_path:
            return None
        return urljoin(base_url, relative_path.strip())


class BaseScraperBackend:
    async def scrape(self, zip_code: str, cfg: Config) -> List[Dict[str, Any]]:
        raise NotImplementedError


class MockScraperBackend(BaseScraperBackend):
    async def scrape(self, zip_code: str, cfg: Config) -> List[Dict[str, Any]]:
        await asyncio.sleep(0.1)  # Simulate small async IO delay
        return [
            {
                "address": f"123 Main St, Pomona, CA {zip_code}",
                "price": "$489,000",
                "beds": "3 bd",
                "baths": "2 ba",
                "sqft": "1,850 sqft",
                "detailUrl": f"/homedetails/123-main-st-{zip_code}/1001_zpid/",
            },
            {
                "address": f"456 Oak Ave, Pomona, CA {zip_code}",
                "price": "$625,000",
                "beds": "4 bd",
                "baths": "3 ba",
                "sqft": "2,400 sqft",
                "detailUrl": f"/homedetails/456-oak-ave-{zip_code}/1002_zpid/",
            },
        ]


class HttpScraperBackend(BaseScraperBackend):
    """Real HTTP & BeautifulSoup scraper backend with robust CSS selector parsing."""

    @staticmethod
    def parse_html(html_content: str, base_url: str, cfg: Config) -> List[Dict[str, Any]]:
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            logger.error("bs4 (BeautifulSoup4) is required for HttpScraperBackend. Install via `pip install beautifulsoup4`.")
            return []

        soup = BeautifulSoup(html_content, "html.parser")
        cards = soup.select(cfg.card_selector)
        extracted = []

        for card in cards:
            # Address extraction
            addr_el = card.select_one(cfg.address_selector)
            address_text = addr_el.get_text(strip=True) if addr_el else None

            # Price extraction & parsing
            price_el = card.select_one(cfg.price_selector)
            price_raw = price_el.get_text(strip=True) if price_el else None
            price_val = ScraperParsingUtils.parse_price(price_raw)

            # Link extraction & resolution
            link_el = card.select_one(cfg.link_selector)
            raw_href = link_el.get("href") if link_el else None
            resolved_url = ScraperParsingUtils.resolve_url(base_url, str(raw_href) if raw_href else None)

            # Details
            beds_el = card.select_one(cfg.beds_selector)
            beds_val = ScraperParsingUtils.parse_number(beds_el.get_text(strip=True) if beds_el else None)

            baths_el = card.select_one(cfg.baths_selector)
            baths_val = ScraperParsingUtils.parse_number(baths_el.get_text(strip=True) if baths_el else None)

            sqft_el = card.select_one(cfg.sqft_selector)
            sqft_val = ScraperParsingUtils.parse_number(sqft_el.get_text(strip=True) if sqft_el else None)

            if address_text:
                extracted.append({
                    "address": address_text,
                    "price_raw": price_raw,
                    "price": price_val,
                    "beds": beds_val,
                    "baths": baths_val,
                    "sqft": sqft_val,
                    "listing_url": resolved_url,
                })

        return extracted

    async def scrape(self, zip_code: str, cfg: Config) -> List[Dict[str, Any]]:
        import urllib.request

        base_url = cfg.source_url_template.format(zip=zip_code)
        logger.info(f"[HttpScraperBackend] Fetching URL: {base_url}")
        
        try:
            # Synchronous fetch wrapped in asyncio executor
            def fetch():
                req = urllib.request.Request(
                    base_url,
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    return resp.read().decode("utf-8")

            loop = asyncio.get_running_loop()
            html = await loop.run_in_executor(None, fetch)
            return self.parse_html(html, base_url, cfg)
        except Exception as e:
            logger.warning(f"[HttpScraperBackend] Network fetch failed for ZIP {zip_code}: {e}. (Falling back gracefully)")
            return []


class PlaywrightScraperBackend(BaseScraperBackend):
    """Headless Chromium backend for JS-rendered listing platforms."""

    async def scrape(self, zip_code: str, cfg: Config) -> List[Dict[str, Any]]:
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("playwright package not installed. Fall back or install via `pip install playwright`.")
            return []

        target_url = cfg.source_url_template.format(zip=zip_code)
        logger.info(f"[PlaywrightScraperBackend] Launching Chromium for {target_url}")

        try:
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page()
                await page.goto(target_url, timeout=15000)
                html_content = await page.content()
                await browser.close()
                return HttpScraperBackend.parse_html(html_content, target_url, cfg)
        except Exception as e:
            logger.error(f"[PlaywrightScraperBackend] Failed scraping {zip_code}: {e}")
            return []


class BaseActor:
    def __init__(self, actor_id: str, system: Optional[ActorSystem] = None):
        self.actor_id = actor_id
        self.system = system
        self.mailbox: asyncio.Queue[ActorMessage] = asyncio.Queue()
        self.is_running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        if self.is_running:
            return
        self.is_running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self.is_running = False
        if self._task and not self._task.done():
            self._task.cancel()

    async def _run_loop(self) -> None:
        while self.is_running:
            try:
                msg = await self.mailbox.get()
                try:
                    await self.receive(msg)
                except Exception as e:
                    logger.error(f"Actor [{self.actor_id}] error processing {msg.action}: {e}", exc_info=True)
                    if self.system and self.actor_id != "supervisor":
                        zip_code = msg.payload.get("zip_code", "unknown")
                        await self.send(
                            "supervisor",
                            "zip_failed",
                            {"zip_code": zip_code, "failed_actor": self.actor_id, "error": str(e)},
                            correlation_id=msg.correlation_id,
                        )
                finally:
                    self.mailbox.task_done()
            except asyncio.CancelledError:
                break

    async def send(self, recipient_id: str, action: str, payload: Dict[str, Any], correlation_id: Optional[str] = None) -> None:
        if not self.system:
            raise RuntimeError(f"Actor [{self.actor_id}] detached from system.")
        msg = ActorMessage(
            sender_id=self.actor_id,
            recipient_id=recipient_id,
            action=action,
            payload=payload,
            correlation_id=correlation_id or str(uuid.uuid4()),
        )
        await self.system.dispatch(msg)

    async def receive(self, msg: ActorMessage) -> None:
        raise NotImplementedError


class ActorSystem:
    def __init__(self, name: str = "RealtorProSystem"):
        self.name = name
        self.actors: Dict[str, BaseActor] = {}

    def register(self, actor: BaseActor) -> BaseActor:
        actor.system = self
        self.actors[actor.actor_id] = actor
        return actor

    async def dispatch(self, msg: ActorMessage) -> None:
        target = self.actors.get(msg.recipient_id)
        if target:
            await target.mailbox.put(msg)
        else:
            logger.warning(f"Target actor [{msg.recipient_id}] not found in registry.")

    async def start_all(self) -> None:
        for actor in self.actors.values():
            await actor.start()

    async def stop_all(self) -> None:
        for actor in self.actors.values():
            await actor.stop()


class ScraperActor(BaseActor):
    def __init__(self, actor_id: str, cfg: Config, tracer: Optional["Tracer"] = None):
        super().__init__(actor_id)
        self.cfg = cfg
        self.tracer = tracer
        # Factory for backend selection
        backends: Dict[str, Type[BaseScraperBackend]] = {
            "mock": MockScraperBackend,
            "http": HttpScraperBackend,
            "playwright": PlaywrightScraperBackend,
        }
        backend_cls = backends.get(cfg.scraper_backend, MockScraperBackend)
        self.backend = backend_cls()
        logger.info(f"[{self.actor_id}] Initialized with backend: '{cfg.scraper_backend}'")

    async def receive(self, msg: ActorMessage) -> None:
        if msg.action == "scrape_zip":
            zip_code = msg.payload.get("zip_code", "")
            logger.info(f"[{self.actor_id}] Scraping listings for ZIP: {zip_code}")
            span = self.tracer.span("scrape_zip", zip=zip_code) if self.tracer else _NullSpan()
            with span:
                raw_items = await self.backend.scrape(zip_code, self.cfg)

            await self.send(
                recipient_id="normalizer",
                action="normalize",
                payload={"zip_code": zip_code, "raw_items": raw_items},
                correlation_id=msg.correlation_id,
            )


class NormalizerActor(BaseActor):
    async def receive(self, msg: ActorMessage) -> None:
        if msg.action == "normalize":
            zip_code = msg.payload.get("zip_code", "")
            raw_items = msg.payload.get("raw_items", [])
            logger.info(f"[{self.actor_id}] Normalizing {len(raw_items)} raw items for ZIP {zip_code}")

            listings = []
            for item in raw_items:
                # Handle numeric price parsing
                raw_price = item.get("price")
                parsed_price = (
                    raw_price if isinstance(raw_price, (int, float))
                    else ScraperParsingUtils.parse_price(str(raw_price))
                )

                # Handle numeric beds/baths/sqft
                beds = item.get("beds")
                baths = item.get("baths")
                sqft = item.get("sqft")

                listings.append(
                    Listing(
                        zip_code=zip_code,
                        address=item.get("address", "Unknown Address"),
                        price=parsed_price,
                        beds=beds if isinstance(beds, (int, float)) else ScraperParsingUtils.parse_number(str(beds)),
                        baths=baths if isinstance(baths, (int, float)) else ScraperParsingUtils.parse_number(str(baths)),
                        sqft=sqft if isinstance(sqft, (int, float)) else ScraperParsingUtils.parse_number(str(sqft)),
                        listing_url=item.get("detailUrl") or item.get("listing_url"),
                    )
                )

            await self.send(
                recipient_id="dedup",
                action="check_dedup",
                payload={"zip_code": zip_code, "listings": listings},
                correlation_id=msg.correlation_id,
            )


def notify_webhook(cfg: "Config", text: str) -> None:
    """Fire-and-forget Discord/Slack-compatible webhook post. No-op if
    NOTIFY_WEBHOOK_URL isn't set; failures are logged, never raised, so a
    dead webhook can't take down the pipeline."""
    if not cfg.notify_webhook_url:
        return
    try:
        import requests

        requests.post(cfg.notify_webhook_url, json={"content": text, "text": text}, timeout=5)
    except Exception as e:
        logger.warning(f"[notify_webhook] Failed to post notification: {e}")


class DedupActor(BaseActor):
    """
    Runs right after normalization, before any paid skip-trace call:
      - address already 'contacted' -> drop it entirely, don't re-spend
        skip-trace budget or re-send outreach
      - address already 'traced' (but not yet contacted) -> reuse the
        cached owner info instead of paying for another lookup
      - otherwise -> pass through untouched as a fresh 'new' lead
    """

    def __init__(self, actor_id: str, cfg: Config, seen_store: "SeenStore"):
        super().__init__(actor_id)
        self.cfg = cfg
        self.seen_store = seen_store

    async def receive(self, msg: ActorMessage) -> None:
        if msg.action != "check_dedup":
            return
        zip_code = msg.payload.get("zip_code", "")
        listings: List[Listing] = msg.payload.get("listings", [])

        forward: List[Listing] = []
        skipped_duplicate = 0

        for listing in listings:
            if self.seen_store.is_contacted(listing.address):
                listing.lead_status = "skipped_duplicate"
                skipped_duplicate += 1
                logger.info(f"[{self.actor_id}] Skipping already-contacted: {listing.address}")
                continue

            cached = self.seen_store.cached_owner(listing.address)
            if cached:
                listing.owner_name = cached.get("owner_name") or listing.owner_name
                listing.owner_phone = cached.get("owner_phone") or listing.owner_phone
                listing.owner_email = cached.get("owner_email") or listing.owner_email
                listing.skip_trace_status = "cached"
                listing.lead_status = "traced"
                logger.info(f"[{self.actor_id}] Reusing cached skip-trace for: {listing.address}")

            forward.append(listing)

        logger.info(
            f"[{self.actor_id}] ZIP {zip_code}: {len(forward)} to process, "
            f"{skipped_duplicate} skipped as already-contacted duplicates"
        )

        await self.send(
            recipient_id="skip_tracer",
            action="trace_listings",
            payload={"zip_code": zip_code, "listings": forward, "skipped_duplicate": skipped_duplicate},
            correlation_id=msg.correlation_id,
        )


class SkipTraceActor(BaseActor):
    def __init__(self, actor_id: str, cfg: Config, tracer: Optional["Tracer"] = None, seen_store: Optional["SeenStore"] = None):
        super().__init__(actor_id)
        self.cfg = cfg
        self.tracer = tracer
        self.seen_store = seen_store
        self.calls_made = 0
        self._last_call_at = 0.0

    async def receive(self, msg: ActorMessage) -> None:
        if msg.action == "trace_listings":
            zip_code = msg.payload.get("zip_code", "")
            listings: List[Listing] = msg.payload.get("listings", [])
            skipped_duplicate = msg.payload.get("skipped_duplicate", 0)
            logger.info(f"[{self.actor_id}] Skip tracing {len(listings)} listings for ZIP {zip_code}")

            for listing in listings:
                if listing.skip_trace_status == "cached":
                    continue  # DedupActor already filled this in, don't re-spend budget

                span = self.tracer.span("skip_trace", address=listing.address) if self.tracer else _NullSpan()
                with span:
                    await self._trace_with_guard(listing)

                if listing.skip_trace_status in ("success", "mocked"):
                    listing.lead_status = "traced"
                    if self.seen_store:
                        self.seen_store.upsert(
                            listing.address,
                            lead_status="traced",
                            owner_name=listing.owner_name,
                            owner_phone=listing.owner_phone,
                            owner_email=listing.owner_email,
                        )
                    if listing.skip_trace_status == "success" and (listing.owner_phone or listing.owner_email):
                        notify_webhook(
                            self.cfg,
                            f"🏠 New owner found: {listing.owner_name or 'Unknown'} @ {listing.address}",
                        )

            if self.seen_store:
                self.seen_store.save()

            await self.send(
                recipient_id="store",
                action="persist_listings",
                payload={"zip_code": zip_code, "listings": listings, "skipped_duplicate": skipped_duplicate},
                correlation_id=msg.correlation_id,
            )

    async def _trace_with_guard(self, listing: Listing) -> None:
        """Cost guard + rate limit wrapper around the real provider call."""
        if self.cfg.skip_trace_provider == "mock" or not self.cfg.skip_trace_api_key:
            listing.owner_name = "Jane Doe (Mock Owner)"
            listing.owner_phone = "555-0199"
            listing.owner_email = "owner@example.com"
            listing.skip_trace_status = "mocked"
            return

        if self.calls_made >= self.cfg.skip_trace_max_calls_per_run:
            logger.warning(
                f"[{self.actor_id}] Skip-trace budget exhausted "
                f"({self.cfg.skip_trace_max_calls_per_run} calls/run) - skipping {listing.address}"
            )
            listing.skip_trace_status = "budget_exceeded"
            return

        # Rate limit: enforce a minimum spacing between real provider calls
        elapsed = time.time() - self._last_call_at
        wait = self.cfg.skip_trace_min_interval_secs - elapsed
        if wait > 0:
            await asyncio.sleep(wait)

        self.calls_made += 1
        self._last_call_at = time.time()
        await self._trace_with_retries(listing)

    async def _trace_with_retries(self, listing: Listing) -> None:
        last_error: Optional[Exception] = None
        for attempt in range(1, self.cfg.skip_trace_max_retries + 1):
            try:
                if self.cfg.skip_trace_provider == "batchdata":
                    await self._trace_batchdata(listing)
                else:
                    logger.warning(
                        f"Unknown SKIP_TRACE_PROVIDER='{self.cfg.skip_trace_provider}', "
                        "falling back to mock trace for this listing."
                    )
                    listing.owner_name = "Jane Doe (Mock Owner)"
                    listing.skip_trace_status = "mocked"
                return  # success or a clean "no_match" - either way, stop retrying
            except _RetryableSkipTraceError as e:
                last_error = e
                backoff = self.cfg.skip_trace_backoff_base_secs * (2 ** (attempt - 1))
                logger.warning(
                    f"[{self.actor_id}] Skip trace attempt {attempt}/{self.cfg.skip_trace_max_retries} "
                    f"failed for {listing.address}: {e}. Retrying in {backoff:.1f}s"
                )
                if attempt < self.cfg.skip_trace_max_retries:
                    await asyncio.sleep(backoff)

        logger.error(f"[{self.actor_id}] Skip trace exhausted retries for {listing.address}: {last_error}")
        listing.skip_trace_status = "failed"

    async def _trace_batchdata(self, listing: Listing) -> None:
        """
        BatchData property-lookup API. Docs: https://developer.batchdata.com
        Endpoint/payload shape below is BatchData's documented "Property Lookup"
        request format as of this writing - verify against their current docs
        before relying on it, since third-party APIs do change. Raises
        _RetryableSkipTraceError on transient failures so the retry wrapper
        above can back off and try again.
        """
        import requests

        def _call():
            return requests.post(
                "https://api.batchdata.com/api/v1/property/lookup/all-attributes",
                headers={
                    "Authorization": f"Bearer {self.cfg.skip_trace_api_key}",
                    "Content-Type": "application/json",
                },
                json={"requests": [{"address": {"street": listing.address}}]},
                timeout=15,
            )

        try:
            resp = await asyncio.get_running_loop().run_in_executor(None, _call)
        except Exception as e:
            raise _RetryableSkipTraceError(f"network error: {e}") from e

        if resp.status_code == 429 or resp.status_code >= 500:
            raise _RetryableSkipTraceError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        if resp.status_code >= 400:
            # Client errors (bad request, auth) won't fix themselves on retry
            listing.skip_trace_status = "failed"
            logger.error(f"BatchData rejected request for {listing.address}: HTTP {resp.status_code}")
            return

        data = resp.json()
        results = (data.get("results") or {}).get("properties") or []
        if not results:
            listing.skip_trace_status = "no_match"
            return

        owner = (results[0].get("owner") or {})
        listing.owner_name = owner.get("fullName") or owner.get("name")
        phones = owner.get("phoneNumbers") or []
        emails = owner.get("emails") or []
        listing.owner_phone = phones[0].get("number") if phones else None
        listing.owner_email = emails[0].get("email") if emails else None
        listing.skip_trace_status = "success"


class _RetryableSkipTraceError(Exception):
    """Raised for transient skip-trace failures (network errors, 429, 5xx)
    that are worth retrying with backoff, as opposed to a clean 4xx reject."""


class StoreActor(BaseActor):
    def __init__(self, actor_id: str, cfg: Config, tracer: Optional["Tracer"] = None):
        super().__init__(actor_id)
        self.cfg = cfg
        self.tracer = tracer
        self._client = None
        if cfg.supabase_url and cfg.supabase_key:
            from supabase import create_client

            self._client = create_client(cfg.supabase_url, cfg.supabase_key)
            logger.info("Supabase connected successfully")

    async def receive(self, msg: ActorMessage) -> None:
        if msg.action == "persist_listings":
            zip_code = msg.payload.get("zip_code", "")
            listings: List[Listing] = msg.payload.get("listings", [])
            skipped_duplicate = msg.payload.get("skipped_duplicate", 0)

            if self.cfg.dry_run:
                logger.info(f"[{self.actor_id}] [DRY RUN] Would persist {len(listings)} listings for ZIP {zip_code}")
                saved_count = 0
            else:
                logger.info(f"[{self.actor_id}] Persisting {len(listings)} listings for ZIP {zip_code} to Supabase")
                saved_count = await self._upsert(listings)

            await self.send(
                recipient_id="outreach",
                action="run_outreach",
                payload={
                    "zip_code": zip_code,
                    "listings": listings,
                    "saved_count": saved_count,
                    "skipped_duplicate": skipped_duplicate,
                },
                correlation_id=msg.correlation_id,
            )

    async def _upsert(self, listings: List[Listing]) -> int:
        if self._client is None:
            logger.warning(f"[{self.actor_id}] Supabase not configured, skipping persistence")
            return 0
        if not listings:
            return 0

        span = self.tracer.span("supabase_upsert", count=len(listings)) if self.tracer else _NullSpan()
        try:
            with span:
                rows = [l.to_dict() for l in listings]

                def _call():
                    return self._client.table("fsbo_listings").upsert(rows, on_conflict="address").execute()

                await asyncio.get_running_loop().run_in_executor(None, _call)
            return len(listings)
        except Exception as e:
            logger.error(f"[{self.actor_id}] Supabase upsert failed: {e}")
            return 0


class OutreachActor(BaseActor):
    def __init__(self, actor_id: str, cfg: Config, tracer: Optional["Tracer"] = None, seen_store: Optional["SeenStore"] = None):
        super().__init__(actor_id)
        self.cfg = cfg
        self.tracer = tracer
        self.seen_store = seen_store
        self.can_send = bool(cfg.gmail_user and cfg.gmail_app_password)
        if not self.can_send:
            logger.info(f"[{self.actor_id}] Gmail not configured - outreach will draft-only, no sends")

    @staticmethod
    def draft(listing: Listing) -> str:
        name = listing.owner_name or "there"
        return (
            f"Hi {name},\n\n"
            f"I noticed your home at {listing.address} is listed for sale by owner. "
            "I work with buyers/sellers in the area and wanted to reach out in case "
            "it would help to have a second set of eyes on pricing, paperwork, or "
            "getting more qualified showings.\n\n"
            "No pressure either way - happy to answer questions if useful.\n\n"
            "Best,\nAlex"
        )

    async def receive(self, msg: ActorMessage) -> None:
        if msg.action == "run_outreach":
            zip_code = msg.payload.get("zip_code", "")
            listings: List[Listing] = msg.payload.get("listings", [])
            saved_count = msg.payload.get("saved_count", 0)
            skipped_duplicate = msg.payload.get("skipped_duplicate", 0)

            sent_count = 0
            for listing in listings:
                if listing.lead_status == "skipped_duplicate":
                    continue  # already handled upstream by DedupActor; belt-and-suspenders
                if self.seen_store and self.seen_store.is_contacted(listing.address):
                    listing.lead_status = "skipped_duplicate"
                    continue

                span = self.tracer.span("outreach", address=listing.address) if self.tracer else _NullSpan()
                with span:
                    await self._send_or_log(listing)

                if listing.outreach_status == "sent":
                    sent_count += 1
                    listing.lead_status = "contacted"
                    if self.seen_store:
                        self.seen_store.upsert(listing.address, lead_status="contacted")

            if self.seen_store:
                self.seen_store.save()

            await self.send(
                recipient_id="supervisor",
                action="zip_completed",
                payload={
                    "zip_code": zip_code,
                    "found_count": len(listings),
                    "saved_count": saved_count,
                    "sent_count": sent_count,
                    "skipped_duplicate": skipped_duplicate,
                },
                correlation_id=msg.correlation_id,
            )

    async def _send_or_log(self, listing: Listing) -> None:
        body = self.draft(listing)
        if self.cfg.dry_run:
            listing.outreach_status = "drafted_only"
            logger.info(f"[{self.actor_id}] [DRY RUN] Would send outreach for: {listing.address}")
            return
        if self.can_send and listing.owner_email:
            try:
                await asyncio.get_running_loop().run_in_executor(None, self._smtp_send, listing, body)
                listing.outreach_status = "sent"
                logger.info(f"[{self.actor_id}] Outreach sent -> {listing.owner_email}")
            except Exception as e:
                listing.outreach_status = "send_failed"
                logger.error(f"[{self.actor_id}] Outreach send failed for {listing.address}: {e}")
        else:
            listing.outreach_status = "drafted_only"
            logger.info(f"[{self.actor_id}] Outreach drafted (no send) for: {listing.address}")

    def _smtp_send(self, listing: Listing, body: str) -> None:
        msg = MIMEText(body)
        msg["Subject"] = f"Re: {listing.address}"
        msg["From"] = self.cfg.gmail_user
        msg["To"] = listing.owner_email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(self.cfg.gmail_user, self.cfg.gmail_app_password)
            server.sendmail(self.cfg.gmail_user, [listing.owner_email], msg.as_string())


class PipelineSupervisor(BaseActor):
    def __init__(self, actor_id: str, target_zips: List[str]):
        super().__init__(actor_id)
        self.target_zips = target_zips
        self.pending_zips = set(target_zips)
        self.results: Dict[str, Dict[str, Any]] = {}
        self.failures: Dict[str, str] = {}
        self.completion_event = asyncio.Event()

    async def receive(self, msg: ActorMessage) -> None:
        if msg.action == "start_pipeline":
            logger.info(f"[{self.actor_id}] Fanning out pipeline across {len(self.target_zips)} ZIP codes concurrently...")
            for zip_code in self.target_zips:
                await self.send(
                    recipient_id="scraper",
                    action="scrape_zip",
                    payload={"zip_code": zip_code},
                    correlation_id=f"corr_{zip_code}",
                )

        elif msg.action == "zip_completed":
            zip_code = msg.payload.get("zip_code", "")
            logger.info(f"[{self.actor_id}] Received completion signal for ZIP: {zip_code}")

            self.results[zip_code] = msg.payload
            self.pending_zips.discard(zip_code)

            if not self.pending_zips:
                logger.info(f"[{self.actor_id}] 🎉 All ZIP codes completed processing!")
                self.completion_event.set()

        elif msg.action == "zip_failed":
            zip_code = msg.payload.get("zip_code", "unknown")
            error = msg.payload.get("error", "unknown error")
            logger.error(f"[{self.actor_id}] ZIP {zip_code} failed permanently: {error}")
            self.failures[zip_code] = error
            self.pending_zips.discard(zip_code)

            if not self.pending_zips:
                self.completion_event.set()


def test_http_scraper_backend_parsing() -> None:
    """Proves HttpScraperBackend's real parsing logic against simulated HTML."""
    logger.info("==========================================================")
    logger.info("   PROVING HttpScraperBackend REAL PARSING LOGIC          ")
    logger.info("==========================================================")

    simulated_html = """
    <html>
        <body>
            <div class="property-card">
                <a class="property-card-link" href="/homedetails/123-main-st-pomona-ca-91766/10001_zpid/">
                    <address class="address">123 Main St, Pomona, CA 91766</address>
                </a>
                <span class="price">$489,000</span>
                <span class="beds">3 bd</span>
                <span class="baths">2 ba</span>
                <span class="sqft">1,850 sqft</span>
            </div>
        </body>
    </html>
    """

    cfg = Config()
    base_url = "https://www.example.com/homes/fsbo/91766_rb/"
    
    parsed_items = HttpScraperBackend.parse_html(simulated_html, base_url, cfg)

    assert len(parsed_items) == 1, f"Expected 1 parsed item, got {len(parsed_items)}"
    item = parsed_items[0]

    # Verification assertions
    assert item["address"] == "123 Main St, Pomona, CA 91766", f"Address mismatch: {item['address']}"
    assert item["price"] == 489000.0, f"Price parsing failed: expected 489000.0, got {item['price']}"
    assert item["listing_url"] == "https://www.example.com/homedetails/123-main-st-pomona-ca-91766/10001_zpid/", f"URL Join failed: {item['listing_url']}"
    assert item["beds"] == 3.0, f"Beds parsing failed: {item['beds']}"
    assert item["baths"] == 2.0, f"Baths parsing failed: {item['baths']}"
    assert item["sqft"] == 1850.0, f"Sqft parsing failed: {item['sqft']}"

    logger.info("✅ PROOF SUCCESSFUL:")
    logger.info(f"   - Address extracted : '{item['address']}'")
    logger.info(f"   - Price parsed      : {item['price_raw']} -> {item['price']} (float)")
    logger.info(f"   - URL Resolved      : '{item['listing_url']}'")
    logger.info(f"   - Specs Extracted   : {item['beds']} beds | {item['baths']} baths | {item['sqft']} sqft")
    logger.info("==========================================================\n")


def _load_zip_codes(args: argparse.Namespace) -> List[str]:
    if args.zips_file:
        with open(args.zips_file, "r") as f:
            zips = [
                line.strip()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            ]
        return zips
    return args.zips


def _filter_by_cooldown(zip_codes: List[str], cfg: Config, force: bool) -> List[str]:
    """Scheduler support: skip ZIPs scraped within the cooldown window unless
    --force is passed. State lives in a small local JSON file - no cron/queue
    infrastructure required to get 'don't rescrape the same ZIP every 5
    minutes' behavior."""
    import json as _json

    if force:
        return zip_codes

    state: Dict[str, float] = {}
    if os.path.exists(cfg.scrape_state_file):
        try:
            with open(cfg.scrape_state_file, "r") as f:
                state = _json.load(f)
        except Exception:
            state = {}

    cooldown_secs = cfg.rescrape_cooldown_hours * 3600
    now = time.time()
    due = []
    for zip_code in zip_codes:
        last = state.get(zip_code)
        if last is None or (now - last) >= cooldown_secs:
            due.append(zip_code)
        else:
            remaining_h = (cooldown_secs - (now - last)) / 3600
            logger.info(
                f"⏭  ZIP {zip_code} scraped {(now - last) / 3600:.1f}h ago, "
                f"still in cooldown for {remaining_h:.1f}h more (use --force to override)"
            )
    return due


def _record_scrape_time(zip_codes: List[str], cfg: Config) -> None:
    import json as _json

    state: Dict[str, float] = {}
    if os.path.exists(cfg.scrape_state_file):
        try:
            with open(cfg.scrape_state_file, "r") as f:
                state = _json.load(f)
        except Exception:
            state = {}
    now = time.time()
    for zip_code in zip_codes:
        state[zip_code] = now
    try:
        with open(cfg.scrape_state_file, "w") as f:
            _json.dump(state, f, indent=2)
    except Exception as e:
        logger.warning(f"Failed to write {cfg.scrape_state_file}: {e}")


def _print_summary_table(results: Dict[str, Dict[str, Any]], failures: Dict[str, str]) -> None:
    headers = ["ZIP", "Found", "Saved", "Traced Sent", "Dupes Skipped", "Status"]
    rows = []
    for zip_code, stats in results.items():
        rows.append([
            zip_code,
            str(stats.get("found_count", 0)),
            str(stats.get("saved_count", 0)),
            str(stats.get("sent_count", 0)),
            str(stats.get("skipped_duplicate", 0)),
            "OK",
        ])
    for zip_code, error in failures.items():
        rows.append([zip_code, "-", "-", "-", "-", f"FAILED: {error[:40]}"])

    if not rows:
        print("(no ZIP codes processed)")
        return

    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]
    line = "+-" + "-+-".join("-" * w for w in widths) + "-+"

    def fmt_row(cells: List[str]) -> str:
        return "| " + " | ".join(c.ljust(w) for c, w in zip(cells, widths)) + " |"

    print(line)
    print(fmt_row(headers))
    print(line)
    for row in rows:
        print(fmt_row(row))
    print(line)


async def async_main(zip_codes: List[str], cfg: Config) -> tuple[Dict[str, Dict[str, Any]], Dict[str, str]]:
    # 1. Run parsing proof test first
    test_http_scraper_backend_parsing()

    tracer = Tracer(cfg)
    seen_store = SeenStore(cfg.dedup_state_file)
    system = ActorSystem("RealtorProActorSystem")

    missing = cfg.missing_required()
    if missing:
        logger.error(f"Missing/placeholder config, some stages will degrade: {', '.join(missing)}")
    if cfg.dry_run:
        logger.info("🧪 DRY RUN MODE: no writes to Supabase, no outreach sends")

    # 2. Instantiate and register actors
    supervisor = system.register(PipelineSupervisor("supervisor", zip_codes))
    system.register(ScraperActor("scraper", cfg, tracer))
    system.register(NormalizerActor("normalizer"))
    system.register(DedupActor("dedup", cfg, seen_store))
    system.register(SkipTraceActor("skip_tracer", cfg, tracer, seen_store))
    system.register(StoreActor("store", cfg, tracer))
    system.register(OutreachActor("outreach", cfg, tracer, seen_store))

    # 3. Start actor loops
    await system.start_all()

    try:
        # 4. Trigger workflow
        await system.dispatch(
            ActorMessage(
                sender_id="main",
                recipient_id="supervisor",
                action="start_pipeline",
            )
        )

        # 5. Await pipeline fan-in completion
        await asyncio.wait_for(supervisor.completion_event.wait(), timeout=cfg.max_items_per_zip * 10 + 30)

    except asyncio.TimeoutError:
        logger.error("Pipeline timed out waiting for ZIP processing completion.")
        for zip_code in supervisor.pending_zips:
            supervisor.failures[zip_code] = "timeout"
    finally:
        await system.stop_all()

    if not cfg.dry_run:
        _record_scrape_time(list(supervisor.results.keys()), cfg)

    return supervisor.results, supervisor.failures


def main() -> None:
    parser = argparse.ArgumentParser(description="Actor-based Realtor Pro FSBO Pipeline")
    parser.add_argument("--zips", nargs="+", default=["91766", "78701", "78702"], help="ZIP codes to process")
    parser.add_argument("--zips-file", help="Path to a file of ZIP codes, one per line (# comments allowed)")
    parser.add_argument("--backend", choices=["mock", "http", "playwright"], help="Override SCRAPER_BACKEND for this run")
    parser.add_argument("--dry-run", action="store_true", help="Run the full pipeline but skip Supabase writes and outreach sends")
    parser.add_argument("--force", action="store_true", help="Ignore the rescrape cooldown and process every requested ZIP")
    parser.add_argument("--watch", type=float, metavar="MINUTES", help="Repeat the run every N minutes until interrupted (Ctrl+C)")
    args = parser.parse_args()

    cfg = Config()
    configure_log_format(cfg)
    if args.backend:
        cfg.scraper_backend = args.backend
    cfg.dry_run = args.dry_run

    zip_codes = _load_zip_codes(args)

    def run_once() -> int:
        due_zips = _filter_by_cooldown(zip_codes, cfg, force=args.force)
        if not due_zips:
            logger.info("Nothing due to (re)scrape right now - all ZIPs are within cooldown.")
            return 0

        results, failures = asyncio.run(async_main(due_zips, cfg))

        print("\n--- FINAL PIPELINE RUN SUMMARY ---")
        _print_summary_table(results, failures)

        if failures and results:
            return 1  # partial failure
        if failures and not results:
            return 2  # hard failure
        return 0

    if args.watch:
        logger.info(f"👁  Watch mode: re-running every {args.watch} minutes. Ctrl+C to stop.")
        try:
            while True:
                exit_code = run_once()
                logger.info(f"Sleeping {args.watch} minutes until next run...")
                time.sleep(args.watch * 60)
        except KeyboardInterrupt:
            logger.info("Watch mode stopped by user.")
            sys.exit(0)
    else:
        sys.exit(run_once())


if __name__ == "__main__":
    main()