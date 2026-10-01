"""Wires adapter -> normalizer -> store -> webhook, plus history sync, alerts and /health."""

import asyncio
import logging
import os
import random
import time
from collections import Counter
from dataclasses import dataclass, field

import aiohttp
from aiohttp import web

from . import normalize, webhook
from .store import Store, iso, now_ms

log = logging.getLogger("max_mirror")
DAY_MS = 86_400_000
PAGE = 40
CHAT_HISTORY = 49


def _csv(name: str, default: str = "") -> set[str]:
    return {x.strip() for x in os.environ.get(name, default).split(",") if x.strip()}


@dataclass
class Config:
    data_dir: str = "./data"
    webhook_url: str = ""
    webhook_secret: str = ""
    alert_url: str = ""
    history_days: int = 30
    history_rps: float = 1.0
    chats_allow: set = field(default_factory=set)
    chats_deny: set = field(default_factory=set)
    chat_types: set = field(default_factory=lambda: {"dialog", "group", "channel"})
    retention_days: int = 0
    http_port: int = 8080

    @classmethod
    def from_env(cls):
        e = os.environ.get
        return cls(
            data_dir=e("MAX_DATA_DIR", "./data"),
            webhook_url=e("MAX_WEBHOOK_URL", ""),
            webhook_secret=e("MAX_WEBHOOK_SECRET", ""),
            alert_url=e("MAX_ALERT_URL", ""),
            history_days=int(e("MAX_HISTORY_DAYS", "30")),
            history_rps=float(e("MAX_HISTORY_RPS", "1")),
            chats_allow=_csv("MAX_CHATS_ALLOW"),
            chats_deny=_csv("MAX_CHATS_DENY"),
            chat_types=_csv("MAX_CHAT_TYPES", "dialog,group,channel"),
            retention_days=int(e("MAX_RETENTION_DAYS", "0")),
            http_port=int(e("MAX_HTTP_PORT", "8080")),
        )


def info(event: str, level=logging.INFO, **fields):
    log.log(level, event, extra={"fields": fields})


class Service:
    def __init__(self, cfg: Config, adapter=None):
        if cfg.webhook_url and not cfg.webhook_secret:
            raise SystemExit("MAX_WEBHOOK_SECRET is required with MAX_WEBHOOK_URL")
        os.makedirs(cfg.data_dir, mode=0o700, exist_ok=True)
        self.cfg = cfg
        self.store = Store(os.path.join(cfg.data_dir, "max.db"), outbox=bool(cfg.webhook_url))
        if adapter is None:
            from .adapter import MaxAdapter
            adapter = MaxAdapter(cfg.data_dir, cfg.history_rps)
        self.adapter = adapter
        self.adapter.on_event = self.on_event
        self.me = None
        self.state = "starting"
        self.state_since = time.time()
        self.last_event_at = None
        self.metrics = Counter()
        self._alerted = set()

    # --- ingestion ---

    def allowed(self, chat_id: str) -> bool:
        if self.cfg.chats_allow and chat_id not in self.cfg.chats_allow:
            return False
        if chat_id in self.cfg.chats_deny:
            return False
        kind = self.store.chat_type(chat_id)
        # ponytail: a chat not seen yet has no type, so it passes the type filter until resolved
        return not kind or kind in self.cfg.chat_types

    def on_event(self, opcode: int, payload: dict):
        """Sync on purpose: the DB write happens before any await, so events keep arrival order."""
        self.metrics["events_received"] += 1
        self.last_event_at = now_ms()
        raw_id = self.store.raw(opcode, payload)
        try:
            actions = normalize.event(opcode, payload, self.me)
        except Exception as e:
            self.store.raw_error(raw_id, repr(e))
            self.metrics["parse_errors"] += 1
            info("parse_error", logging.WARNING, opcode=opcode, raw_id=raw_id, error=repr(e))
            return
        for action in actions:
            if not self.apply(action, notify=True):
                self.store.drop_raw(raw_id)  # filtered chat: keep nothing of it

    def apply(self, action: tuple, notify: bool) -> bool:
        kind = action[0]
        if kind == "chat":
            self.store.upsert_chat(action[1])
            return True
        chat_id = action[1]["chat_id"] if kind == "message" else action[1]
        if not self.allowed(chat_id):
            return False
        if kind == "message":
            if self.store.save_message(action[1], notify):
                self.metrics["messages_saved"] += 1
        elif kind == "deleted":
            self.store.delete_messages(chat_id, action[2])
        return True

    def ingest_page(self, chat_id: str, payload: dict, notify: bool) -> list[int]:
        """Store a history page; returns the sent_at of every message in it."""
        raw_id = self.store.raw(CHAT_HISTORY, payload)
        times = []
        for raw in payload.get("messages") or []:
            try:
                m = normalize.message(raw, chat_id)
            except Exception as e:
                self.store.raw_error(raw_id, repr(e))
                self.metrics["parse_errors"] += 1
                continue
            times.append(m["sent_at"])
            if m["status"] == "REMOVED":
                self.store.delete_messages(chat_id, [m["message_id"]])
            elif self.store.save_message(m, notify):
                self.metrics["messages_saved"] += 1
        return times

    # --- history (HIST-1..3) ---

    async def sync_history(self):
        for raw in await self.adapter.all_chats():
            self.store.upsert_chat(normalize.chat(raw, self.me))
        self.store.upsert_users([normalize.user(u) for u in self.adapter.contacts()])
        chats = [c for c in self.store.chats() if self.allowed(c["chat_id"])]
        for c in chats:
            await self.gap_fill(c)
        for c in chats:
            if not c["backfill_done"]:
                await self.backfill(c)
        info("history_synced", chats=len(chats))

    async def gap_fill(self, chat):
        """HIST-2: everything after the last stored message, delivered to the webhook as new."""
        chat_id = chat["chat_id"]
        last = self.store.last_sent_at(chat_id)
        if last is None and not chat["backfill_done"]:
            return  # first sync: backfill loads it quietly, without webhook calls
        cursor = last or now_ms() - self.cfg.history_days * DAY_MS
        if chat["last_event_at"] and chat["last_event_at"] <= cursor:
            return
        while True:
            page = await self.adapter.history(chat_id, cursor, forward=PAGE)
            times = self.ingest_page(chat_id, page, notify=True)
            if len(times) < PAGE or max(times) <= cursor:
                return
            cursor = max(times)

    async def backfill(self, chat):
        """HIST-1: walk back N days; the cursor survives restarts (HIST-3)."""
        chat_id = chat["chat_id"]
        stop_at = now_ms() - self.cfg.history_days * DAY_MS
        cursor = chat["backfill_cursor"] or now_ms()
        while cursor > stop_at:
            page = await self.adapter.history(chat_id, cursor, backward=PAGE)
            times = self.ingest_page(chat_id, page, notify=False)
            if len(times) < PAGE or min(times) >= cursor:
                break  # reached the start of the chat
            cursor = min(times)
            self.store.set_backfill(chat_id, cursor, done=False)
        self.store.set_backfill(chat_id, cursor, done=True)
        info("backfill_done", chat_id=chat_id)

    async def resolve_names(self):
        """Senders and chats first seen in events: fetch their names/types once."""
        if ids := self.store.unknown_chat_ids():
            found = [normalize.chat(raw, self.me) for raw in await self.adapter.chats_by_id(ids)]
            missing = set(ids) - {c["chat_id"] for c in found}
            # "" = asked once, server didn't know it; stops re-asking every cycle
            for c in found + [normalize.chat({"id": i, "type": ""}) for i in missing]:
                self.store.upsert_chat(c)
        if ids := self.store.unknown_user_ids():
            found = [normalize.user(u) for u in await self.adapter.users(ids)]
            missing = set(ids) - {u["user_id"] for u in found}
            self.store.upsert_users(found + [{"user_id": i, "display_name": None} for i in missing])

    # --- connection (AUTH-1..3, reconnect) ---

    def set_state(self, state: str):
        # state_since = start of the current up/down period, not of each reconnect attempt
        if (state == "connected") != (self.state == "connected"):
            self.state_since = time.time()
        self.state = state

    async def connection_loop(self):
        from .adapter import SessionRevoked
        delay = 1
        while True:
            self.set_state("connecting")
            try:
                self.me = await self.adapter.connect()
            except SessionRevoked as e:
                self.set_state("session_revoked")
                self.adapter.forget_session()
                await self.alert("session_revoked", f"MAX session revoked ({e}); restart the service and scan the QR")
                return  # AUTH-3: wait for a manual login, no retries
            except Exception as e:
                self.set_state("disconnected")
                info("connect_failed", logging.WARNING, error=repr(e), retry_in_s=delay)
            else:
                connected_at = time.time()
                self.set_state("connected")
                info("auth_ok", account_id=self.me)
                sync = asyncio.create_task(self.sync_history())
                sync.add_done_callback(_log_failure)
                await self.adapter.wait_closed()
                sync.cancel()
                self.set_state("disconnected")
                info("disconnected", logging.WARNING)
                if time.time() - connected_at > 60:
                    delay = 1
            self.metrics["reconnects"] += 1
            await asyncio.sleep(delay * random.uniform(0.5, 1.5))
            delay = min(delay * 2, 300)

    # --- ops (OPS-1, alerts, retention) ---

    async def alert(self, key: str, text: str):
        info("alert", logging.ERROR, key=key, text=text)
        if not self.cfg.alert_url:
            return
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
                await http.post(self.cfg.alert_url, json={"text": f"[max-mirror] {text}"})
        except Exception as e:
            info("alert_failed", logging.WARNING, error=repr(e))

    async def check(self, key: str, firing: bool, text: str):
        """Alert once when a condition starts firing; re-arm when it clears."""
        if firing and key not in self._alerted:
            self._alerted.add(key)
            await self.alert(key, text)
        elif not firing:
            self._alerted.discard(key)

    async def monitor_loop(self):
        last_purge = 0
        while True:
            await asyncio.sleep(30)
            down = self.state in ("connecting", "disconnected") and time.time() - self.state_since > 300
            await self.check("no_connection", down, "no MAX connection for over 5 minutes")
            errors = self.store.parse_errors_since(now_ms() - 3_600_000)
            await self.check("parse_errors", errors > 0, f"{errors} parse errors in the last hour")
            size = self.store.outbox_size()
            await self.check("outbox", size > 1000, f"webhook queue holds {size} events")
            if self.state == "connected":
                await _guard(self.resolve_names())
            if self.cfg.retention_days and time.time() - last_purge > 3600:
                self.store.purge(now_ms() - self.cfg.retention_days * DAY_MS)
                last_purge = time.time()

    def health(self) -> dict:
        age = (now_ms() - self.last_event_at) / 1000 if self.last_event_at else None
        return {
            "status": self.state,
            "account_id": self.me,
            "last_event_at": iso(self.last_event_at),
            "last_event_age_s": age,
            "webhook_queue": self.store.outbox_size(),
            "metrics": dict(self.metrics),
        }

    async def serve_health(self):
        async def handler(_):
            body = self.health()
            return web.json_response(body, status=200 if body["status"] == "connected" else 503)

        app = web.Application()
        app.router.add_get("/health", handler)
        runner = web.AppRunner(app, access_log=None)
        await runner.setup()
        await web.TCPSite(runner, "0.0.0.0", self.cfg.http_port).start()

    async def run(self):
        await self.serve_health()
        tasks = [self.connection_loop(), self.monitor_loop()]
        if self.cfg.webhook_url:
            tasks.append(webhook.deliver_forever(self.store, self.cfg.webhook_url, self.cfg.webhook_secret, self.metrics))
        try:
            await asyncio.gather(*tasks)
        finally:
            await self.adapter.close()


async def _guard(coro):
    try:
        await coro
    except Exception as e:
        info("task_failed", logging.WARNING, error=repr(e))


def _log_failure(task: asyncio.Task):
    if not task.cancelled() and task.exception():
        info("history_sync_failed", logging.WARNING, error=repr(task.exception()))
