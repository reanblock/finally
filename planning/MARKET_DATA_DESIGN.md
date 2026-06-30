# Market Data Backend — Detailed Design

Authoritative, implementation-ready design for the FinAlly market data subsystem. This document describes the system exactly as built and shipped under `backend/app/market/` (8 modules, ~500 lines, 73 passing tests). It is the reference contract for every downstream component — SSE streaming, portfolio valuation, trade execution, watchlist management, and the LLM chat layer.

> **Status:** Implemented, tested (84% coverage), reviewed, all review items resolved. This document reflects the final code, not a pre-implementation sketch. Where the design evolved during review (e.g. top-level `massive` imports, a public `get_tickers()` on the simulator, the SSE generator's return type), this document records the final decision.

---

## Table of Contents

1. [Goals & Constraints](#1-goals--constraints)
2. [Architecture at a Glance](#2-architecture-at-a-glance)
3. [File Structure](#3-file-structure)
4. [Data Model — `models.py`](#4-data-model--modelspy)
5. [Price Cache — `cache.py`](#5-price-cache--cachepy)
6. [Abstract Interface — `interface.py`](#6-abstract-interface--interfacepy)
7. [Seed Prices & Parameters — `seed_prices.py`](#7-seed-prices--parameters--seed_pricespy)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator--simulatorpy)
9. [Massive API Client — `massive_client.py`](#9-massive-api-client--massive_clientpy)
10. [Factory — `factory.py`](#10-factory--factorypy)
11. [SSE Streaming Endpoint — `stream.py`](#11-sse-streaming-endpoint--streampy)
12. [Public API — `__init__.py`](#12-public-api--__init__py)
13. [FastAPI Lifecycle Integration](#13-fastapi-lifecycle-integration)
14. [Watchlist Coordination](#14-watchlist-coordination)
15. [Data Flow & Timing](#15-data-flow--timing)
16. [Error Handling & Edge Cases](#16-error-handling--edge-cases)
17. [Testing Strategy](#17-testing-strategy)
18. [Configuration Summary](#18-configuration-summary)
19. [Integration Checklist for Downstream Agents](#19-integration-checklist-for-downstream-agents)

---

## 1. Goals & Constraints

The market data layer must:

- **Stream live prices** for an arbitrary, mutable set of tickers at a smooth ~500ms cadence.
- **Work out of the box with no API key** — the GBM simulator is the default so students can run the app immediately.
- **Optionally use real data** via the Massive (Polygon.io) REST API when `MASSIVE_API_KEY` is set, with zero changes to any downstream code.
- **Decouple producers from consumers** — the SSE endpoint, portfolio valuation, and trade execution all read from one shared cache and never know or care which source is active.
- **Be a good background citizen** — long-running tasks that start and stop cleanly with the FastAPI lifespan, survive transient errors, and never leak.
- **Support future multi-user scenarios** without a data-layer rewrite (the cache is keyed by ticker today; the architecture generalizes).

Non-goals: order books, historical bars/candles from the simulator (the frontend accumulates its own sparkline history from the SSE stream), WebSockets, or per-client price filtering (in the single-user model every client sees every tracked ticker).

---

## 2. Architecture at a Glance

```
                 ┌─────────────────────────────────────────┐
                 │           MarketDataSource (ABC)         │
                 │   start / stop / add / remove / tickers  │
                 └─────────────────────────────────────────┘
                       ▲                          ▲
          ┌────────────┘                          └────────────┐
┌─────────────────────┐                      ┌──────────────────────┐
│ SimulatorDataSource │                      │   MassiveDataSource   │
│  GBM @ 500ms tick   │                      │  Polygon poll @ 15s   │
│  (default, no key)  │                      │  (when API key set)   │
└─────────────────────┘                      └──────────────────────┘
          │                                              │
          └──────────────────┬───────────────────────────┘
                             ▼  writes
                 ┌───────────────────────────┐
                 │   PriceCache (thread-safe) │   single point of truth
                 │   latest PriceUpdate/tick  │   + monotonic version counter
                 └───────────────────────────┘
                             │  reads
          ┌──────────────────┼─────────────────────┐
          ▼                  ▼                     ▼
  SSE /api/stream/prices   Portfolio valuation   Trade execution
  (version-gated push)     (get_price)           (get_price at fill)
```

**Key principle:** the data source is a *producer* that pushes into the cache on its own clock. Everything else is a *consumer* that reads the cache on its own clock. The two are never directly coupled, which is why swapping the simulator for the Massive poller requires no downstream changes.

**Patterns used:**

| Pattern | Where | Why |
|---|---|---|
| Strategy | `MarketDataSource` + 2 impls | Source-agnostic downstream code |
| Factory | `create_market_data_source()` | Env-driven selection at one place |
| Producer/consumer w/ shared store | `PriceCache` | Decouples update cadence from read cadence |
| Immutable value object | `PriceUpdate` | Safe to share across async tasks/threads |
| Router factory (closure DI) | `create_stream_router()` | Inject the cache without module globals |

---

## 3. File Structure

```
backend/
  app/
    market/
      __init__.py          # Public re-exports (the only import surface)
      models.py            # PriceUpdate dataclass
      cache.py             # PriceCache (thread-safe in-memory store)
      interface.py         # MarketDataSource ABC
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, correlation constants
      simulator.py         # GBMSimulator (math) + SimulatorDataSource (async wrapper)
      massive_client.py    # MassiveDataSource (Polygon.io REST poller)
      factory.py           # create_market_data_source()
      stream.py            # create_stream_router() — SSE endpoint
  tests/
    market/
      test_models.py
      test_cache.py
      test_simulator.py
      test_simulator_source.py
      test_factory.py
      test_massive.py
```

Each module has a single responsibility. Downstream code imports **only** from `app.market` (the package `__init__`), never reaching into submodules.

---

## 4. Data Model — `models.py`

`PriceUpdate` is the **only** type that crosses the market-data boundary. Every consumer works with this object.

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds

    @property
    def change(self) -> float:
        """Absolute price change from the previous update."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percentage change from the previous update."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat'."""
        if self.price > self.previous_price:
            return "up"
        elif self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE transmission."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
        }
```

### Design decisions

- **`frozen=True`** — value objects never mutate; safe to share across async tasks and threads without copying.
- **`slots=True`** — these are created at high frequency (every tick, every ticker); slots trim per-instance memory.
- **Derived properties** (`change`, `change_percent`, `direction`) are computed from `price`/`previous_price`, so they can never be inconsistent or stale.
- **`to_dict()`** is the single serialization point shared by the SSE endpoint and any REST response that returns prices.

> **`previous_price` semantics:** it is the price from the *immediately preceding update* for that ticker, set by `PriceCache` (see §5). It is **not** the prior trading day's close. The frontend's per-ticker "daily change %" should be computed from the first price observed on the current page load, not from this field. `change`/`direction` here drive the per-tick flash animation.

---

## 5. Price Cache — `cache.py`

The central data hub. Producers write; consumers read. It must be thread-safe because the Massive client's synchronous SDK call runs in `asyncio.to_thread()` (a real OS thread), while SSE reads happen on the event loop.

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe in-memory cache of the latest price for each ticker.

    Writers: SimulatorDataSource or MassiveDataSource (one at a time).
    Readers: SSE streaming endpoint, portfolio valuation, trade execution.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version: int = 0  # Monotonically increasing; bumped on every update

    def update(self, ticker: str, price: float, timestamp: float | None = None) -> PriceUpdate:
        """Record a new price for a ticker. Returns the created PriceUpdate.

        Computes direction/change from the previous price automatically.
        On the first update for a ticker, previous_price == price (direction='flat').
        """
        with self._lock:
            ts = timestamp or time.time()
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_all(self) -> dict[str, PriceUpdate]:
        """Snapshot of all current prices. Returns a shallow copy."""
        with self._lock:
            return dict(self._prices)

    def get_price(self, ticker: str) -> float | None:
        """Convenience: just the price float, or None."""
        update = self.get(ticker)
        return update.price if update else None

    def remove(self, ticker: str) -> None:
        with self._lock:
            self._prices.pop(ticker, None)

    @property
    def version(self) -> int:
        """Monotonic counter. Bumped on every update; drives SSE change detection."""
        return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### The version counter

The SSE loop polls the cache every ~500ms. Without a change signal it would re-serialize and re-send every price every tick — wasteful, and especially pointless when Massive only refreshes every 15s. The monotonic `version` lets the SSE loop send **only when something actually changed**:

```python
last_version = -1
while True:
    if cache.version != last_version:
        last_version = cache.version
        yield format_sse(cache.get_all())
    await asyncio.sleep(0.5)
```

### Why `threading.Lock` (not `asyncio.Lock`)

- The Massive SDK is synchronous and runs via `asyncio.to_thread()` → a real OS thread. An `asyncio.Lock` would **not** protect against that thread.
- A `threading.Lock` is correct from both the event loop and worker threads.
- The critical section is a dict lookup + assignment — contention is negligible at this scale (≤ dozens of tickers, ~2 writes/sec).

> **Note on `version`:** it reads `self._version` without the lock. On CPython this int read is atomic under the GIL, so it's safe today. If the project ever targets a free-threaded (no-GIL) build, wrap the read in the lock. Left as-is intentionally to keep the read path allocation-free.

---

## 6. Abstract Interface — `interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push price updates into a shared PriceCache on their own
    schedule. Downstream code never calls the data source for prices — it
    reads from the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Begin producing updates for the given tickers. Call exactly once."""

    @abstractmethod
    async def stop(self) -> None:
        """Stop the background task and release resources. Safe to call repeatedly."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the active set. No-op if already present."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the active set, and from the PriceCache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Return the current list of actively tracked tickers."""
```

**Contract guarantees both implementations honor:**

| Method | Guarantee |
|---|---|
| `start(tickers)` | Seeds the cache with at least one price per ticker *before returning* (or after the first poll), so consumers have data immediately. Called once. |
| `stop()` | Idempotent. Cancels the background task and awaits it; never raises on double-call. |
| `add_ticker(t)` | Idempotent. The new ticker appears in the cache promptly (simulator: immediately; Massive: on the next poll). |
| `remove_ticker(t)` | Idempotent. Removes from the active set **and** the cache. |
| `get_tickers()` | Synchronous; returns a copy of the active set. |

---

## 7. Seed Prices & Parameters — `seed_prices.py`

Constants only — no logic, no non-stdlib imports. Shared by the simulator (initial prices, GBM params, correlation structure).

```python
"""Seed prices and per-ticker parameters for the market simulator."""

# Realistic starting prices for the default watchlist
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00,
    "TSLA": 250.00, "NVDA": 800.00, "META": 500.00, "JPM": 195.00,
    "V": 280.00, "NFLX": 600.00,
}

# Per-ticker GBM parameters
#   sigma: annualized volatility (higher = more movement)
#   mu:    annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL":  {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT":  {"sigma": 0.20, "mu": 0.05},
    "AMZN":  {"sigma": 0.28, "mu": 0.05},
    "TSLA":  {"sigma": 0.50, "mu": 0.03},   # High volatility
    "NVDA":  {"sigma": 0.40, "mu": 0.08},   # High volatility, strong drift
    "META":  {"sigma": 0.30, "mu": 0.05},
    "JPM":   {"sigma": 0.18, "mu": 0.04},   # Low volatility (bank)
    "V":     {"sigma": 0.17, "mu": 0.04},   # Low volatility (payments)
    "NFLX":  {"sigma": 0.35, "mu": 0.05},
}

# Defaults for dynamically-added tickers not in the table above
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

# Correlation groups for the Cholesky decomposition
CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

# Correlation coefficients
INTRA_TECH_CORR = 0.6      # Tech stocks move together
INTRA_FINANCE_CORR = 0.5   # Finance stocks move together
CROSS_GROUP_CORR = 0.3     # Between sectors / unknown tickers
TSLA_CORR = 0.3            # TSLA does its own thing
```

> **Naming note (resolved in review):** there is a single fallback constant, `CROSS_GROUP_CORR` (0.3), used for cross-sector pairs *and* unknown tickers. An earlier draft had a separate `DEFAULT_CORR`; it was removed to avoid two same-valued constants with overlapping meaning.

Tuning rationale: `sigma` values mirror real-world relative volatility (TSLA 0.50 ≫ V 0.17). The tiny per-tick `dt` (see §8) means these annualized figures produce sub-cent moves per 500ms tick that accumulate into a realistic intraday range.

---

## 8. GBM Simulator — `simulator.py`

Two classes:

- **`GBMSimulator`** — pure, synchronous math engine. Stateful: holds current prices and advances them one `step()` at a time.
- **`SimulatorDataSource`** — the `MarketDataSource` implementation that drives `GBMSimulator` in an async loop and writes to the `PriceCache`.

### 8.1 The math

At each step, each price evolves by Geometric Brownian Motion:

```
S(t+dt) = S(t) · exp( (mu − ½σ²)·dt  +  σ·√dt·Z )
```

- `S(t)` current price · `mu` annualized drift · `sigma` annualized vol · `dt` time step (fraction of a trading year) · `Z` a **correlated** standard normal draw.
- `dt = 0.5 / (252 · 6.5 · 3600) ≈ 8.48e-8` (500ms over a 252-day, 6.5-hour trading year).

GBM is multiplicative through `exp(...)`, so **prices can never go negative** — a key correctness property.

### 8.2 Correlated moves via Cholesky

Real sectors move together. We build a correlation matrix `C` from sector membership, factor it as `L = cholesky(C)`, and turn independent normals into correlated ones:

```
Z_correlated = L @ Z_independent
```

The matrix is rebuilt whenever the ticker set changes (O(n²), trivial for n < 50). Cholesky requires `C` to be positive-definite, which a valid correlation matrix is.

### 8.3 `GBMSimulator`

```python
from __future__ import annotations

import asyncio
import logging
import math
import random

import numpy as np

from .cache import PriceCache
from .interface import MarketDataSource
from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS,
    INTRA_FINANCE_CORR, INTRA_TECH_CORR, SEED_PRICES, TICKER_PARAMS, TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600   # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def step(self) -> dict[str, float]:
        """Advance all tickers by one step. Returns {ticker: new_price}. Hot path."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu = self._params[ticker]["mu"]
            sigma = self._params[ticker]["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            # Random shock: ~0.1%/tick/ticker → a visible event roughly every ~50s
            # across 10 tickers at 2 ticks/sec.
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock

            result[ticker] = round(self._prices[ticker], 2)
        return result

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = corr[j, i] = rho
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]
        if t1 == "TSLA" or t2 == "TSLA":          # TSLA is a loner (it's in `tech`,
            return TSLA_CORR                       # but checked first to override)
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

**Notes**

- The `_add_ticker_internal` / `_rebuild_cholesky` split lets `__init__` add N tickers and rebuild the matrix **once**, instead of N times.
- `get_tickers()` is public on `GBMSimulator` (added in review) so `SimulatorDataSource` doesn't reach into private state.
- Unknown tickers seed at a random `$50–$300` and use `DEFAULT_PARAMS`.
- The TSLA check precedes the tech check on purpose — TSLA is a member of the `tech` set but should correlate at 0.3, not 0.6.

### 8.4 `SimulatorDataSource`

```python
class SimulatorDataSource(MarketDataSource):
    """MarketDataSource backed by the GBM simulator. Ticks every `update_interval`s."""

    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        # Seed the cache with initial prices so SSE has data on its very first tick.
        for ticker in tickers:
            price = self._sim.get_price(ticker)
            if price is not None:
                self._cache.update(ticker=ticker, price=price)
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")
        logger.info("Simulator started with %d tickers", len(tickers))

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        logger.info("Simulator stopped")

    async def add_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.add_ticker(ticker)
            price = self._sim.get_price(ticker)
            if price is not None:                       # seed immediately → tradeable now
                self._cache.update(ticker=ticker, price=price)
            logger.info("Simulator: added ticker %s", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)
        logger.info("Simulator: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker=ticker, price=price)
            except Exception:
                logger.exception("Simulator step failed")  # never let one bad tick kill the feed
            await asyncio.sleep(self._interval)
```

**Behaviors that matter downstream**

- **Immediate seeding** — `start()` and `add_ticker()` write a price to the cache *before* the loop next runs, so there's no blank-screen gap and a freshly-added ticker is tradeable right away.
- **Graceful cancellation** — `stop()` cancels and awaits the task, swallowing `CancelledError` for a clean lifespan teardown.
- **Per-tick resilience** — the loop catches exceptions per iteration; a single bad step logs and continues.

---

## 9. Massive API Client — `massive_client.py`

Polls the Massive (Polygon.io) REST snapshot endpoint for **all** watched tickers in one call per cycle, then writes results to the cache. The SDK is synchronous, so each fetch runs in `asyncio.to_thread()` to keep the event loop free.

```python
from __future__ import annotations

import asyncio
import logging

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Rate limits:
      - Free tier (5 req/min) → poll every 15s (default)
      - Paid tiers → poll every 2-5s
    """

    def __init__(self, api_key: str, price_cache: PriceCache, poll_interval: float = 15.0) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(tickers)
        await self._poll_once()  # immediate first poll → cache has data right away
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            logger.info("Massive: added ticker %s (appears on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = ticker.upper().strip()
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internal ---

    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots)
            processed = 0
            for snap in snapshots:
                try:
                    price = snap.last_trade.price
                    timestamp = snap.last_trade.timestamp / 1000.0  # ms → seconds
                    self._cache.update(ticker=snap.ticker, price=price, timestamp=timestamp)
                    processed += 1
                except (AttributeError, TypeError) as e:
                    logger.warning("Skipping snapshot for %s: %s",
                                   getattr(snap, "ticker", "???"), e)
            logger.debug("Massive poll: updated %d/%d tickers", processed, len(self._tickers))
        except Exception as e:
            logger.error("Massive poll failed: %s", e)
            # Don't re-raise; retry next interval. Typical: 401 (key), 429 (rate), network.

    def _fetch_snapshots(self) -> list:
        """Synchronous SDK call; runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=self._tickers,
        )
```

### Resilience matrix

| Failure | Behavior |
|---|---|
| 401 Unauthorized (bad key) | Logged at `error`. Poller keeps running; cache keeps last-known prices. |
| 429 Rate limited | Logged. Retries after `poll_interval`. (Default 15s respects the free tier's 5/min.) |
| Network timeout / transient | Logged. Retries on the next cycle. |
| One malformed snapshot | That ticker skipped with a `warning`; all other tickers still update. |
| Whole poll fails | Cache retains last-known prices — stale data beats no data; SSE keeps streaming. |

### Imports at module level (resolved in review)

`massive` is a **core dependency** in `pyproject.toml`, so `RESTClient` and `SnapshotMarketType` are imported at the top of the module, not lazily inside methods. This makes mocking straightforward (`patch("app.market.massive_client.RESTClient")` targets a real module attribute) and matches the dependency reality.

### Timestamp units

Massive returns `last_trade.timestamp` in **Unix milliseconds**; divide by 1000 to get the seconds that `PriceCache`/`PriceUpdate` expect. The simulator path uses `time.time()` (seconds) by default, so both sources produce consistent timestamps.

---

## 10. Factory — `factory.py`

One place decides which source to instantiate, based solely on `MASSIVE_API_KEY`.

```python
from __future__ import annotations

import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Select the data source from the environment.

    MASSIVE_API_KEY set & non-empty → MassiveDataSource (real data)
    otherwise                       → SimulatorDataSource (GBM simulation)

    Returns an *unstarted* source; the caller must `await source.start(tickers)`.
    """
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        logger.info("Market data source: Massive API (real data)")
        return MassiveDataSource(api_key=api_key, price_cache=price_cache)
    logger.info("Market data source: GBM Simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

A whitespace-only `MASSIVE_API_KEY` is treated as unset (`.strip()`), so a stray space in `.env` won't accidentally select the real API.

---

## 11. SSE Streaming Endpoint — `stream.py`

A FastAPI route holding a long-lived `text/event-stream` connection, pushing all tracked prices to the client. Built via a **router factory** so the `PriceCache` is injected by closure (no module globals to wire up).

```python
from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/stream", tags=["streaming"])


def create_stream_router(price_cache: PriceCache) -> APIRouter:
    """Build the SSE router bound to a specific PriceCache."""

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        return StreamingResponse(
            _generate_events(price_cache, request),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable nginx buffering if proxied
            },
        )

    return router


async def _generate_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
) -> AsyncGenerator[str, None]:
    """Yield SSE-formatted price events until the client disconnects."""
    yield "retry: 1000\n\n"  # browser EventSource reconnect delay (ms)

    last_version = -1
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while True:
            if await request.is_disconnected():
                logger.info("SSE client disconnected: %s", client_ip)
                break

            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                prices = price_cache.get_all()
                if prices:
                    data = {t: u.to_dict() for t, u in prices.items()}
                    yield f"data: {json.dumps(data)}\n\n"

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        logger.info("SSE stream cancelled for: %s", client_ip)
```

### Wire format

Each event is a single JSON object keyed by ticker:

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.50,"previous_price":190.42,"timestamp":1707580800.5,"change":0.08,"change_percent":0.042,"direction":"up"},"GOOGL":{...}}

```

Client:

```javascript
const es = new EventSource('/api/stream/prices');
es.onmessage = (event) => {
  const prices = JSON.parse(event.data);   // { AAPL: {price, change, direction, ...}, ... }
  // flash green/red on `direction`, append `price` to that ticker's sparkline buffer
};
es.onerror = () => { /* EventSource auto-reconnects per the retry directive */ };
```

### Design points

- **Version-gated sends** — the loop wakes every 500ms but only emits when `version` changed, avoiding redundant payloads (important on the 15s Massive cadence).
- **Disconnect detection** — `request.is_disconnected()` ends the generator promptly when the browser tab closes, freeing the task.
- **`retry: 1000`** — instructs `EventSource` to reconnect ~1s after a drop (the connection-status dot in the header can go yellow→green around this).
- **`X-Accel-Buffering: no`** — prevents nginx/proxies from buffering the stream if the app is deployed behind one.
- **Poll-and-push (not event-driven)** — produces evenly-spaced updates, which the frontend relies on for clean, regularly-sampled sparklines.

> **Testing footgun:** `router` is a module-level singleton and `create_stream_router()` registers `/prices` on it via closure. Calling the factory twice in one process (e.g. across tests) double-registers the route. In production it's called exactly once at startup. If you need to call it repeatedly in tests, construct a fresh `APIRouter` inside the factory instead.

---

## 12. Public API — `__init__.py`

The only import surface for the rest of the backend:

```python
"""Market data subsystem for FinAlly.

Public API:
    PriceUpdate                - Immutable price snapshot dataclass
    PriceCache                 - Thread-safe in-memory price store
    MarketDataSource           - Abstract interface for data providers
    create_market_data_source  - Factory (simulator vs Massive)
    create_stream_router       - FastAPI SSE router factory
"""

from .cache import PriceCache
from .factory import create_market_data_source
from .interface import MarketDataSource
from .models import PriceUpdate
from .stream import create_stream_router

__all__ = [
    "PriceUpdate",
    "PriceCache",
    "MarketDataSource",
    "create_market_data_source",
    "create_stream_router",
]
```

Downstream code does:

```python
from app.market import (
    PriceCache, PriceUpdate, MarketDataSource,
    create_market_data_source, create_stream_router,
)
```

---

## 13. FastAPI Lifecycle Integration

The subsystem starts and stops with the app via the `lifespan` context manager. This is the **one** place that wires the cache, the source, and the SSE router together.

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.market import (
    PriceCache, MarketDataSource,
    create_market_data_source, create_stream_router,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    price_cache = PriceCache()
    app.state.price_cache = price_cache

    source = create_market_data_source(price_cache)
    app.state.market_source = source

    initial_tickers = await load_watchlist_tickers()   # read from SQLite watchlist
    await source.start(initial_tickers)

    app.include_router(create_stream_router(price_cache))

    yield  # ---- app runs ----

    # --- SHUTDOWN ---
    await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)


# DI helpers for route handlers
def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

> **Startup ordering:** the database must be lazily initialized/seeded (default watchlist) **before** `load_watchlist_tickers()` runs, so the source starts with the 10 default tickers. If the watchlist is empty, `start([])` is valid — both sources no-op until a ticker is added.

Consumers use the DI helpers:

```python
from fastapi import APIRouter, Depends, HTTPException

router = APIRouter(prefix="/api")


@router.post("/portfolio/trade")
async def execute_trade(trade: TradeRequest,
                        cache: PriceCache = Depends(get_price_cache)):
    price = cache.get_price(trade.ticker)
    if price is None:
        raise HTTPException(400, f"Price not yet available for {trade.ticker}.")
    # ... fill at `price`, update positions/cash, snapshot portfolio ...


@router.post("/watchlist")
async def add_to_watchlist(payload: WatchlistAdd,
                           source: MarketDataSource = Depends(get_market_source)):
    # ... insert into watchlist table ...
    await source.add_ticker(payload.ticker)


@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str,
                                source: MarketDataSource = Depends(get_market_source)):
    # ... delete from watchlist table ...
    await source.remove_ticker(ticker)   # but see §14 for the open-position guard
```

---

## 14. Watchlist Coordination

Whenever the watchlist changes — through the REST API or the LLM chat tool-calls — the data source must be told to track the right set of tickers.

**Add flow**

```
POST /api/watchlist {ticker:"PYPL"}
  → INSERT into watchlist (SQLite)
  → await source.add_ticker("PYPL")
       simulator: add to GBM, rebuild Cholesky, seed cache (tradeable immediately)
       massive:   append to poll set (appears on next poll)
  → return ticker + current price (if available yet)
```

**Remove flow**

```
DELETE /api/watchlist/PYPL
  → DELETE from watchlist (SQLite)
  → await source.remove_ticker("PYPL")
       both: drop from active set and from PriceCache
```

### Edge case: removing a ticker you still hold

If a user removes a watchlist ticker but still has an open position, the source must keep tracking it so portfolio valuation stays correct. Guard it:

```python
@router.delete("/watchlist/{ticker}")
async def remove_from_watchlist(ticker: str,
                                source: MarketDataSource = Depends(get_market_source)):
    await db.delete_watchlist_entry(ticker)
    position = await db.get_position(ticker)
    if position is None or position.quantity == 0:
        await source.remove_ticker(ticker)   # only stop tracking if no exposure
    return {"status": "ok"}
```

Conversely, when a **buy** creates a position for a ticker that isn't on the watchlist, call `source.add_ticker(...)` so the position has a live price even if it was never explicitly watched. Normalize tickers (`.upper().strip()`) at the API boundary to stay consistent with `MassiveDataSource`.

---

## 15. Data Flow & Timing

```
SIMULATOR PATH (default)
  every 0.5s:  GBMSimulator.step() → cache.update(t, price)  [bumps version]
  every 0.5s:  SSE loop: version changed? → push all prices

MASSIVE PATH (MASSIVE_API_KEY set)
  every 15s:   to_thread(get_snapshot_all) → cache.update(...) per ticker
  every 0.5s:  SSE loop: version changed? → push (so ~1 push per 15s of real data)
```

| Clock | Owner | Default | Tunable via |
|---|---|---|---|
| Simulator tick | `SimulatorDataSource` | 500 ms | `update_interval` |
| Massive poll | `MassiveDataSource` | 15 s | `poll_interval` |
| SSE push check | `_generate_events` | 500 ms | `interval` arg |
| Browser reconnect | SSE `retry:` | 1000 ms | literal in `stream.py` |

The three clocks are independent by design — the cache + version counter is the only synchronization point.

---

## 16. Error Handling & Edge Cases

| Scenario | Handling |
|---|---|
| **Empty watchlist at startup** | `start([])` is valid; simulator produces nothing, Massive skips its call; SSE sends nothing until a ticker is added. |
| **Trade before price exists** (just-added ticker on Massive, pre-first-poll) | `get_price()` returns `None`; the trade route returns HTTP 400 with a "try again in a moment" message. The simulator avoids this by seeding on `add_ticker`. |
| **Invalid Massive key** | First poll 401s; logged; poller keeps running with empty cache. SSE is "connected" but carries no data. Fix the key and restart. |
| **Massive 429 / network blip** | Logged; retried next cycle; last-known prices stay in the cache. |
| **Malformed snapshot** | That ticker skipped (`AttributeError`/`TypeError` caught); others still update. |
| **Simulator bad tick** | `_run_loop` catches per-iteration and continues. |
| **Client disconnect** | `request.is_disconnected()` ends the generator; task is freed. |
| **App shutdown mid-stream** | Task cancellation raises `CancelledError`, caught and logged; `source.stop()` joins the background task. |
| **Negative prices** | Impossible — GBM is multiplicative via `exp()`. |
| **Float precision** | Prices are `round(..., 2)`; the exponential form is numerically stable. |
| **Concurrency** | All cache mutations are under `threading.Lock`; the Massive fetch runs off-loop via `to_thread`. |

---

## 17. Testing Strategy

73 tests across 6 modules; 84% line coverage. Run from `backend/`:

```bash
uv sync --extra dev
uv run --extra dev pytest -v
uv run --extra dev pytest --cov=app
uv run --extra dev ruff check app/ tests/
```

`pyproject.toml` sets `asyncio_mode = "auto"`, so `async def test_*` functions run without per-test `@pytest.mark.asyncio`.

| Module | What it covers |
|---|---|
| `test_models.py` | `change`/`change_percent`/`direction` math; flat/up/down; zero-prev guard; `to_dict()`; immutability. |
| `test_cache.py` | update/get/get_all/remove/get_price; first-update-is-flat; version increments; `__len__`/`__contains__`. |
| `test_simulator.py` | `step()` keys; positivity over 10k steps; seed prices; add/remove (incl. no-ops); unknown-ticker random seed; empty step; Cholesky `None` at n≤1, built at n≥2; drift over time. |
| `test_simulator_source.py` | `start()` seeds cache immediately; prices move over time; double-`stop()` clean; add/remove updates both source and cache. |
| `test_factory.py` | Returns simulator when key absent/blank/whitespace; returns Massive when key set. |
| `test_massive.py` | `_poll_once` updates cache (mocked snapshots); malformed snapshot skipped; API error doesn't crash; ms→s timestamp conversion. |

**Mocking Massive without live API:** patch the module-level `RESTClient` (`patch("app.market.massive_client.RESTClient")`) for start/stop, and patch `source._fetch_snapshots` to return fake snapshot objects for poll logic. Example fake:

```python
def _make_snapshot(ticker, price, ts_ms):
    snap = MagicMock()
    snap.ticker = ticker
    snap.last_trade.price = price
    snap.last_trade.timestamp = ts_ms   # milliseconds
    return snap
```

Coverage is intentionally lower for `massive_client.py` (real API methods can't run in CI) and `stream.py` (SSE needs a running ASGI server). A future improvement is one `httpx.AsyncClient` integration test that connects to `/api/stream/prices` and asserts at least one `data:` frame.

---

## 18. Configuration Summary

| Parameter | Location | Default | Meaning |
|---|---|---|---|
| `MASSIVE_API_KEY` | env (`.env`) | `""` | Set → Massive API; unset/blank → simulator |
| `update_interval` | `SimulatorDataSource.__init__` | `0.5 s` | Simulator tick period |
| `event_probability` | `GBMSimulator` / `SimulatorDataSource` | `0.001` | Per-tick/per-ticker shock chance |
| `dt` | `GBMSimulator` | `~8.48e-8` | GBM step (fraction of a trading year) |
| `poll_interval` | `MassiveDataSource.__init__` | `15.0 s` | Massive poll period (free-tier safe) |
| SSE push check | `_generate_events(interval=)` | `0.5 s` | Cache poll cadence for pushes |
| SSE retry | `stream.py` literal | `1000 ms` | Browser reconnect delay |

Relevant `pyproject.toml`:

```toml
dependencies = [
    "fastapi>=0.115.0",
    "uvicorn[standard]>=0.32.0",
    "numpy>=2.0.0",
    "massive>=1.0.0",
    "rich>=13.0.0",
]

[tool.hatch.build.targets.wheel]
packages = ["app"]            # required for `uv sync` / Docker build to succeed

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

---

## 19. Integration Checklist for Downstream Agents

**Portfolio / trade agent**

- [ ] Read fill prices with `cache.get_price(ticker)`; on `None`, return HTTP 400 (price not ready).
- [ ] Total portfolio value = cash + Σ(position.qty × `cache.get_price(ticker)`); skip/flag tickers with no price.
- [ ] After a buy that creates a new position, `await source.add_ticker(ticker)` so it has a live price.
- [ ] Record a `portfolio_snapshots` row immediately after each trade (and on the 30s timer).

**Watchlist agent**

- [ ] On add: DB insert → `await source.add_ticker(t)`.
- [ ] On remove: DB delete → `await source.remove_ticker(t)` **only if no open position** (§14).
- [ ] Normalize tickers with `.upper().strip()` at the boundary.

**Frontend agent**

- [ ] Connect with `new EventSource('/api/stream/prices')`; parse `event.data` as `{ticker: {...}}`.
- [ ] Use `direction` for the flash class; accumulate `price` into per-ticker sparkline buffers since page load.
- [ ] Compute the displayed "daily %" from the first price seen this session, not `previous_price` (which is per-tick).
- [ ] Drive the connection-status dot from `EventSource` `onopen`/`onerror`.

**App/lifespan owner**

- [ ] Initialize/seed the DB before `load_watchlist_tickers()`.
- [ ] Create cache → `create_market_data_source(cache)` → `await source.start(tickers)` → `include_router(create_stream_router(cache))`.
- [ ] Store `cache` and `source` on `app.state`; expose via DI helpers.
- [ ] `await source.stop()` on shutdown.

**Import discipline**

- [ ] Import only from `app.market` (the package), never from submodules.

---

*This document is the source of truth for the market data subsystem. If the implementation and this document diverge, treat the running code in `backend/app/market/` as canonical and update this document to match.*
