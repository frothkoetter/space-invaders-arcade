#!/usr/bin/env python3
"""Continuous Space Invaders NiFi load sim — runs until Ctrl+C.

Each simulated player loops full game sessions (kills, score, hits, level.completed,
optional bunker.friendly_fire) and POSTs like the browser: nifiPostBody batching
(flush every 500 ms, up to 50 events), text/plain JSON by default.

Environment:
  NIFI_URL, PLAYER_COUNT (10–100 or 50-100), CONCURRENCY, DRY_RUN=1
  RUN_ONCE=1 or --once  one-shot batch mode (legacy)
  FAIL_FAST=1  exit on first POST failure (continuous mode)
  NIFI_FLUSH_MS, NIFI_BATCH_MAX  override game NiFi queue (500 / 50)
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any
from urllib import error, request

GAME_VERSION = "flag-edition-v20-init-order-fix"
DEFAULT_NIFI_URL = (
    "http://projtriton.inbound.dfx.hahgzjod.a465-9q4k.cloudera.site:9088/space-invaders"
)
DEFAULT_CONTENT_TYPE = "text/plain;charset=UTF-8"
PLAYER_MIN, PLAYER_MAX = 10, 100
INVADERS_PER_LEVEL = 55
POINT_VALUES = (10, 20, 30, 40)
NIFI_FLUSH_MS_DEFAULT = 500
NIFI_BATCH_MAX_DEFAULT = 50
HTTP_POST_TIMEOUT_SEC = 15

shutdown = threading.Event()
_user_stop = False
_post_pool: ThreadPoolExecutor | None = None
_post_fail_logged: set[str] = set()
_post_fail_lock = threading.Lock()


def env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def nifi_post_body(events: list[dict[str, Any]]) -> dict[str, Any] | list[dict[str, Any]]:
    return events[0] if len(events) == 1 else events


def clamp_players(n: int) -> int:
    if n < PLAYER_MIN or n > PLAYER_MAX:
        print(
            f"warning: player count {n} outside [{PLAYER_MIN},{PLAYER_MAX}]; clamping",
            file=sys.stderr,
        )
    return max(PLAYER_MIN, min(PLAYER_MAX, n))


def parse_int_env(name: str, default: str) -> int:
    raw = os.environ.get(name, default).strip()
    try:
        return int(raw)
    except ValueError as e:
        raise SystemExit(f"{name} must be an integer (got {raw!r})") from e


def resolve_player_count(spec: str) -> int:
    spec = spec.strip()
    range_m = re.fullmatch(r"(\d+)\s*[-–]\s*(\d+)", spec)
    if range_m:
        lo, hi = int(range_m.group(1)), int(range_m.group(2))
        if lo > hi:
            lo, hi = hi, lo
        n = random.randint(lo, hi)
        print(f"PLAYER_COUNT range {spec!r} → {n} players", file=sys.stderr)
        return clamp_players(n)
    try:
        return clamp_players(int(spec))
    except ValueError as e:
        raise SystemExit(
            f"PLAYER_COUNT must be an integer ({PLAYER_MIN}–{PLAYER_MAX}) or range like 50-100; got {spec!r}"
        ) from e


def random_player_name(i: int) -> str:
    tags = ("PILOT", "ACE", "ROOK", "VIPER", "PHOENIX", "ORBIT", "NEXUS", "DELTA")
    return f"{random.choice(tags)}-{i:03d}"


@dataclass
class SimStats:
    lock: threading.Lock = field(default_factory=threading.Lock)
    events_emitted: int = 0
    posts_ok: int = 0
    posts_fail: int = 0
    posts_skipped_shutdown: int = 0

    def add_events(self, n: int) -> None:
        with self.lock:
            self.events_emitted += n

    def add_post(self, ok: bool) -> None:
        with self.lock:
            if ok:
                self.posts_ok += 1
            else:
                self.posts_fail += 1

    def add_skipped(self, n: int = 1) -> None:
        with self.lock:
            self.posts_skipped_shutdown += n


def log_post_failure(label: str, detail: str) -> None:
    if shutdown.is_set():
        return
    with _post_fail_lock:
        if label in _post_fail_logged:
            return
        _post_fail_logged.add(label)
    print(f"POST fail {label}: {detail}", file=sys.stderr)


def http_post_batch(
    url: str,
    content_type: str,
    body_obj: dict[str, Any] | list[dict[str, Any]],
    *,
    dry_run: bool,
    label: str,
    fail_fast: bool,
) -> tuple[bool, str]:
    if shutdown.is_set() and not dry_run:
        return False, "shutdown"

    body = json.dumps(body_obj, separators=(",", ":"))
    if dry_run:
        preview = body if len(body) <= 280 else body[:280] + "…"
        print(f"[dry-run] {label} ({len(body_obj) if isinstance(body_obj, list) else 1} events): {preview}")
        return True, "dry-run"

    req = request.Request(
        url,
        data=body.encode("utf-8"),
        method="POST",
        headers={"Content-Type": content_type, "Accept": "application/json"},
    )
    try:
        with request.urlopen(req, timeout=HTTP_POST_TIMEOUT_SEC) as resp:
            ok = 200 <= resp.getcode() < 300
            if not ok and fail_fast:
                shutdown.set()
            return ok, f"HTTP {resp.getcode()}"
    except error.HTTPError as e:
        if fail_fast:
            shutdown.set()
        return False, f"HTTP {e.code}"
    except error.URLError as e:
        reason = e.reason
        detail = str(reason)
        if fail_fast:
            shutdown.set()
        return False, detail


class NifiPoster:
    """Per-player NiFi queue — matches NIFI_STREAM flushIntervalMs / batchMax."""

    def __init__(
        self,
        *,
        url: str,
        content_type: str,
        dry_run: bool,
        label: str,
        executor: ThreadPoolExecutor,
        stats: SimStats,
        flush_ms: int,
        batch_max: int,
        fail_fast: bool,
    ) -> None:
        self.url = url
        self.content_type = content_type
        self.dry_run = dry_run
        self.label = label
        self.executor = executor
        self.stats = stats
        self.flush_ms = flush_ms
        self.batch_max = batch_max
        self.fail_fast = fail_fast
        self._queue: list[dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

    def enqueue(self, event: dict[str, Any]) -> None:
        if shutdown.is_set():
            return
        with self._lock:
            if shutdown.is_set():
                return
            self._queue.append(event)
            if len(self._queue) >= self.batch_max:
                self._flush_locked()

    def maybe_flush(self) -> None:
        if shutdown.is_set():
            return
        with self._lock:
            if not self._queue or shutdown.is_set():
                return
            if (time.monotonic() - self._last_flush) * 1000 >= self.flush_ms:
                self._flush_locked()

    def flush_all(self) -> None:
        if shutdown.is_set():
            self._discard_queue()
            return
        with self._lock:
            while self._queue and not shutdown.is_set():
                self._flush_locked()

    def _discard_queue(self) -> None:
        with self._lock:
            if self._queue:
                n = len(self._queue)
                self._queue.clear()
                self.stats.add_skipped(n)

    def _flush_locked(self) -> None:
        if shutdown.is_set():
            return
        batch = self._queue[: self.batch_max]
        del self._queue[: len(batch)]
        self._last_flush = time.monotonic()
        body = nifi_post_body(batch)
        n = len(batch)
        self.stats.add_events(n)

        def do_post() -> None:
            if shutdown.is_set():
                self.stats.add_skipped(1)
                return
            ok, detail = http_post_batch(
                self.url,
                self.content_type,
                body,
                dry_run=self.dry_run,
                label=self.label,
                fail_fast=self.fail_fast,
            )
            self.stats.add_post(ok)
            if not ok and detail != "shutdown":
                log_post_failure(self.label, detail)

        try:
            self.executor.submit(do_post)
        except RuntimeError:
            self.stats.add_skipped(1)


@dataclass
class GameSession:
    session_id: str
    level: int = 1
    lives: int = 3
    score: int = 0
    frame: int = 0
    remaining_invaders: int = INVADERS_PER_LEVEL
    level_start_ms: int = 0

    def new_session(self) -> None:
        self.session_id = str(uuid.uuid4())
        self.level = 1
        self.lives = 3
        self.score = 0
        self.frame = random.randint(0, 120)
        self.remaining_invaders = INVADERS_PER_LEVEL
        self.level_start_ms = int(time.time() * 1000)


def make_event(
    *,
    player_id: str,
    player_name: str,
    session: GameSession,
    event_type: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    session.frame += random.randint(4, 18)
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": int(time.time() * 1000),
        "session_id": session.session_id,
        "player_id": player_id,
        "player_name": player_name,
        "game_version": GAME_VERSION,
        "level": session.level,
        "frame": session.frame,
        "data_json": json.dumps(data, separators=(",", ":")),
    }


def sim_invader_kill(session: GameSession) -> list[dict[str, Any]]:
    pts = random.choice(POINT_VALUES)
    prev = session.score
    session.score += pts
    session.remaining_invaders = max(0, session.remaining_invaders - 1)
    row, col = random.randint(0, 4), random.randint(0, 10)
    return [
        ("invader.killed", {
            "invaderRow": row,
            "invaderCol": col,
            "flag": random.choice(("AT", "BE", "US")),
            "pointsAwarded": pts,
            "position": {"x": random.randint(40, 760), "y": random.randint(80, 400)},
            "remainingInvaders": session.remaining_invaders,
        }),
        ("score.changed", {
            "previousScore": prev,
            "newScore": session.score,
            "delta": pts,
            "reason": "invader_kill",
        }),
    ]


def sim_player_hit(session: GameSession) -> list[tuple[str, dict[str, Any]]]:
    prev_lives = session.lives
    session.lives -= 1
    game_over = session.lives <= 0
    return [
        ("player.destroyed", {
            "position": {"x": random.randint(100, 700), "y": 520},
            "cause": "invader_bullet",
            "livesRemainingAfter": session.lives,
            "gameOver": game_over,
        }),
        ("lives.changed", {
            "previousLives": prev_lives,
            "newLives": session.lives,
            "reason": "player_hit",
        }),
    ]


def sim_level_completed(session: GameSession) -> tuple[str, dict[str, Any]]:
    now = int(time.time() * 1000)
    payload = {
        "completedLevel": session.level,
        "nextLevel": session.level + 1,
        "scoreAtCompletion": session.score,
        "livesRemaining": session.lives,
        "durationMs": now - session.level_start_ms,
    }
    session.level += 1
    session.remaining_invaders = INVADERS_PER_LEVEL
    session.level_start_ms = now
    return ("level.completed", payload)


def sim_bunker_friendly_fire() -> tuple[str, dict[str, Any]]:
    return ("bunker.friendly_fire", {
        "bunkerIndex": random.randint(0, 3),
        "bunkerPosition": {"x": random.randint(80, 600), "y": 460},
        "pixelHit": {"row": random.randint(0, 5), "col": random.randint(0, 11)},
        "pixelsDestroyed": random.randint(1, 9),
        "bulletPosition": {"x": random.randint(100, 700), "y": random.randint(430, 480)},
    })


def run_player_loop(
    index: int,
    player_id: str,
    player_name: str,
    poster: NifiPoster,
    *,
    think_min: float,
    think_max: float,
) -> None:
    session = GameSession(session_id=str(uuid.uuid4()))
    session.new_session()

    while not shutdown.is_set():
        session.new_session()
        while session.lives > 0 and session.remaining_invaders > 0 and not shutdown.is_set():
            poster.maybe_flush()

            roll = random.random()
            if roll < 0.04:
                et, data = sim_bunker_friendly_fire()
                poster.enqueue(
                    make_event(
                        player_id=player_id,
                        player_name=player_name,
                        session=session,
                        event_type=et,
                        data=data,
                    )
                )
            elif roll < 0.12:
                for et, data in sim_player_hit(session):
                    poster.enqueue(
                        make_event(
                            player_id=player_id,
                            player_name=player_name,
                            session=session,
                            event_type=et,
                            data=data,
                        )
                    )
                if session.lives <= 0:
                    break
            else:
                for et, data in sim_invader_kill(session):
                    poster.enqueue(
                        make_event(
                            player_id=player_id,
                            player_name=player_name,
                            session=session,
                            event_type=et,
                            data=data,
                        )
                    )
                if session.remaining_invaders == 0:
                    et, data = sim_level_completed(session)
                    poster.enqueue(
                        make_event(
                            player_id=player_id,
                            player_name=player_name,
                            session=session,
                            event_type=et,
                            data=data,
                        )
                    )

            if shutdown.wait(random.uniform(think_min, think_max)):
                break

        poster.flush_all()
        if shutdown.is_set():
            break
        if shutdown.wait(random.uniform(1.5, 4.0)):
            break

    poster.flush_all()


def build_session_events_one_shot(
    *,
    player_id: str,
    player_name: str,
    session_id: str,
    count: int,
) -> list[dict[str, Any]]:
    """Legacy single POST payload for --once."""
    session = GameSession(session_id=session_id)
    session.new_session()
    poster_events: list[dict[str, Any]] = []

    def push(pairs: list[tuple[str, dict[str, Any]]]) -> None:
        for et, data in pairs:
            poster_events.append(
                make_event(
                    player_id=player_id,
                    player_name=player_name,
                    session=session,
                    event_type=et,
                    data=data,
                )
            )

    while len(poster_events) < count and session.lives > 0:
        if len(poster_events) >= count:
            break
        push(sim_invader_kill(session))
        if session.remaining_invaders == 0:
            push([sim_level_completed(session)])
        if len(poster_events) >= count - 2 and session.lives > 0 and random.random() < 0.4:
            push(sim_player_hit(session))
    return poster_events[:count]


def cancel_post_pool() -> None:
    global _post_pool
    pool = _post_pool
    if pool is not None:
        pool.shutdown(wait=False, cancel_futures=True)


def install_signal_handlers() -> None:
    def stop(_signum: int, _frame: object) -> None:
        global _user_stop
        if shutdown.is_set():
            print("\nForce exit.", flush=True)
            cancel_post_pool()
            sys.exit(130)
        print("\nShutting down…", flush=True)
        _user_stop = True
        shutdown.set()
        cancel_post_pool()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Simulate Space Invaders players POSTing to NiFi (continuous until Ctrl+C)",
    )
    p.add_argument("--nifi-url", default=os.environ.get("NIFI_URL", DEFAULT_NIFI_URL))
    p.add_argument(
        "--player-count",
        dest="player_count_spec",
        default=os.environ.get("PLAYER_COUNT", "25"),
        metavar="N or MIN-MAX",
    )
    p.add_argument(
        "--concurrency",
        type=int,
        default=parse_int_env("CONCURRENCY", "8"),
        help="max parallel HTTP POSTs",
    )
    p.add_argument(
        "--once",
        action="store_true",
        default=os.environ.get("RUN_ONCE", "").strip() in ("1", "true", "yes"),
        help="one batch per player then exit (legacy)",
    )
    p.add_argument(
        "--events-per-player",
        type=int,
        default=parse_int_env("EVENTS_PER_PLAYER", "") if os.environ.get("EVENTS_PER_PLAYER") else None,
    )
    p.add_argument("--events-min", type=int, default=parse_int_env("EVENTS_MIN", "5"))
    p.add_argument("--events-max", type=int, default=parse_int_env("EVENTS_MAX", "20"))
    p.add_argument(
        "--dry-run",
        action="store_true",
        default=os.environ.get("DRY_RUN", "").strip() in ("1", "true", "yes"),
    )
    p.add_argument("--content-type", default=os.environ.get("NIFI_CONTENT_TYPE", DEFAULT_CONTENT_TYPE))
    p.add_argument("--seed", type=int, default=None)
    p.add_argument(
        "--think-min",
        type=float,
        default=float(os.environ.get("SIM_THINK_MIN_SEC", "0.08")),
        help="min seconds between in-game actions per player",
    )
    p.add_argument(
        "--think-max",
        type=float,
        default=float(os.environ.get("SIM_THINK_MAX_SEC", "0.35")),
    )
    return p.parse_args()


def join_player_threads(threads: list[threading.Thread], timeout_sec: float) -> None:
    deadline = time.monotonic() + timeout_sec
    for t in threads:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        t.join(timeout=remaining)


def print_final_stats(stats: SimStats, *, interrupted: bool) -> None:
    with stats.lock:
        skipped = stats.posts_skipped_shutdown
        line = (
            f"Stopped. events={stats.events_emitted} "
            f"POST ok={stats.posts_ok} fail={stats.posts_fail}"
        )
        if skipped:
            line += f" skipped_after_shutdown={skipped}"
    print(line, flush=True)
    if interrupted and stats.posts_fail:
        print(
            "Tip: connection errors usually mean NiFi is not listening at NIFI_URL "
            "(wrong host/port/path, gateway down, or ListenHTTP not running).",
            file=sys.stderr,
        )


def run_continuous(args: argparse.Namespace, player_count: int) -> int:
    global _post_pool
    flush_ms = parse_int_env("NIFI_FLUSH_MS", str(NIFI_FLUSH_MS_DEFAULT))
    batch_max = parse_int_env("NIFI_BATCH_MAX", str(NIFI_BATCH_MAX_DEFAULT))
    fail_fast = env_flag("FAIL_FAST")
    stats = SimStats()
    install_signal_handlers()

    print(f"NIFI_URL = {args.nifi_url}", flush=True)
    print(
        f"Continuous sim: {player_count} players\n"
        f"NiFi batch flush {flush_ms}ms / max {batch_max}; Content-Type: {args.content_type}\n"
        f"Press Ctrl+C to stop (Ctrl+C again to force exit).",
        flush=True,
    )
    if fail_fast:
        print("FAIL_FAST=1 — first POST failure stops the sim.", flush=True)

    post_pool = ThreadPoolExecutor(max_workers=max(1, args.concurrency))
    _post_pool = post_pool
    player_threads: list[threading.Thread] = []
    try:
        for i in range(1, player_count + 1):
            player_id = f"sim-{uuid.uuid4()}"
            player_name = random_player_name(i)
            poster = NifiPoster(
                url=args.nifi_url,
                content_type=args.content_type,
                dry_run=args.dry_run,
                label=f"player {i} ({player_name})",
                executor=post_pool,
                stats=stats,
                flush_ms=flush_ms,
                batch_max=batch_max,
                fail_fast=fail_fast,
            )
            t = threading.Thread(
                target=run_player_loop,
                args=(i, player_id, player_name, poster),
                kwargs={"think_min": args.think_min, "think_max": args.think_max},
                name=f"sim-player-{i}",
                daemon=True,
            )
            t.start()
            player_threads.append(t)

        while not shutdown.is_set():
            shutdown.wait(2.0)
            if shutdown.is_set():
                break
            with stats.lock:
                print(
                    f"  … {stats.events_emitted} events queued/sent, "
                    f"POST ok={stats.posts_ok} fail={stats.posts_fail}",
                    flush=True,
                )
    finally:
        shutdown.set()
        cancel_post_pool()
        join_player_threads(player_threads, 2.0)
        post_pool.shutdown(wait=False, cancel_futures=True)
        _post_pool = None

    print_final_stats(stats, interrupted=_user_stop)
    if fail_fast and stats.posts_fail:
        return 1
    return 0


def run_once(args: argparse.Namespace, player_count: int) -> int:
    from concurrent.futures import as_completed

    fail_fast = env_flag("FAIL_FAST")
    jobs: list[tuple[int, str, list[dict[str, Any]]]] = []
    for i in range(1, player_count + 1):
        player_id = f"sim-{uuid.uuid4()}"
        session_id = f"sim-session-{uuid.uuid4()}"
        player_name = random_player_name(i)
        n = args.events_per_player or random.randint(args.events_min, args.events_max)
        events = build_session_events_one_shot(
            player_id=player_id,
            player_name=player_name,
            session_id=session_id,
            count=n,
        )
        jobs.append((i, player_name, events))

    total = sum(len(j[2]) for j in jobs)
    print(f"NIFI_URL = {args.nifi_url}", flush=True)
    print(f"One-shot: {player_count} players, {total} events")

    ok_count = 0
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = []
        for i, player_name, events in jobs:
            label = f"player {i} ({player_name})"
            futures.append(
                (
                    label,
                    pool.submit(
                        http_post_batch,
                        args.nifi_url,
                        args.content_type,
                        nifi_post_body(events),
                        dry_run=args.dry_run,
                        label=label,
                        fail_fast=fail_fast,
                    ),
                )
            )
        for label, fut in futures:
            ok, detail = fut.result()
            if ok:
                ok_count += 1
            elif not ok:
                log_post_failure(label, detail)

    print(f"Done: {ok_count}/{len(jobs)} POSTs ok")
    return 0 if ok_count == len(jobs) else 1


def main() -> int:
    args = parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    player_count = resolve_player_count(args.player_count_spec)
    if args.once:
        return run_once(args, player_count)
    return run_continuous(args, player_count)


if __name__ == "__main__":
    sys.exit(main())
