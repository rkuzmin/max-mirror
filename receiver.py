"""Test webhook receiver: verifies X-Signature and prints events.

    MAX_WEBHOOK_SECRET=... uv run python receiver.py [port]
"""

import json
import os
import sys

from aiohttp import web

from max_mirror.webhook import sign

SECRET = os.environ["MAX_WEBHOOK_SECRET"]


async def hook(request):
    body = await request.read()
    if request.headers.get("X-Signature") != sign(SECRET, body):
        print("REJECTED: bad signature", flush=True)
        return web.Response(status=401)
    e = json.loads(body)
    m = e["message"]
    print(e["event"], e["chat"]["id"], m["id"], m["type"], repr(m["text"][:60]), flush=True)
    return web.Response(status=204)


app = web.Application()
app.router.add_post("/", hook)
web.run_app(app, port=int(sys.argv[1]) if len(sys.argv) > 1 else 9000)
