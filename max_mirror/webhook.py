"""Outbox -> webhook: at-least-once, in order per chat, HMAC-signed (OUT-1..3)."""

import asyncio
import hashlib
import hmac
import json
import logging

import aiohttp

from .store import Store, now_ms

log = logging.getLogger("max_mirror.webhook")
GIVE_UP_MS = 24 * 3600 * 1000
MAX_DELAY_S = 300


def sign(secret: str, body: bytes) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def deliver_forever(store: Store, url: str, secret: str, metrics: dict):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as http:
        while True:
            due = store.due_outbox(now_ms())
            for row in due:
                body = json.dumps(store.enrich(json.loads(row["payload"])), ensure_ascii=False).encode()
                headers = {"Content-Type": "application/json", "X-Signature": sign(secret, body)}
                try:
                    async with http.post(url, data=body, headers=headers) as resp:
                        ok = 200 <= resp.status < 300
                        reason = resp.status
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    ok, reason = False, type(e).__name__

                if ok:
                    store.outbox_done(row["id"], "delivered_at")
                    metrics["webhook_delivered"] += 1
                elif now_ms() - row["created_at"] > GIVE_UP_MS:
                    store.outbox_done(row["id"], "failed_at")
                    metrics["webhook_failed"] += 1
                    log.error("webhook_gave_up", extra={"fields": {"outbox_id": row["id"], "reason": reason}})
                else:
                    attempts = row["attempts"] + 1
                    delay = min(2 ** (attempts - 1), MAX_DELAY_S)
                    store.outbox_retry(row["id"], attempts, now_ms() + delay * 1000)
                    log.warning("webhook_retry", extra={"fields": {"outbox_id": row["id"], "reason": reason,
                                                                   "attempt": attempts, "delay_s": delay}})
            if not due:
                await asyncio.sleep(1)
