"""max-mirror run | export --chat ID --from DATE --to DATE [--out FILE]"""

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone

from .service import Config, Service
from .store import Store


class JsonFormatter(logging.Formatter):
    def format(self, record):
        entry = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": record.getMessage(),
            **getattr(record, "fields", {}),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False, default=str)


def setup_logging():
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=os.environ.get("MAX_LOG_LEVEL", "INFO"), handlers=[handler], force=True)
    # PyMax logs frames (tokens, message text) at DEBUG: keep it at WARNING whatever our level is
    pymax = logging.getLogger("pymax")
    pymax.handlers.clear()
    pymax.propagate = True
    pymax.setLevel(logging.WARNING)


def to_ms(text: str, end: bool = False) -> int:
    """Date or datetime (UTC unless an offset is given); a bare --to date covers that whole day."""
    if len(text) == 10:
        day = date.fromisoformat(text) + timedelta(days=1 if end else 0)
        moment = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    else:
        moment = datetime.fromisoformat(text)
        moment = moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp() * 1000)


def main():
    parser = argparse.ArgumentParser(prog="max-mirror")
    sub = parser.add_subparsers(dest="cmd")
    sub.add_parser("run", help="connect and collect messages (default)")
    exp = sub.add_parser("export", help="one chat for a period as JSONL (OUT-4)")
    exp.add_argument("--chat", required=True)
    exp.add_argument("--from", dest="start", required=True, help="YYYY-MM-DD or ISO datetime")
    exp.add_argument("--to", dest="end", required=True, help="YYYY-MM-DD (inclusive) or ISO datetime")
    exp.add_argument("--out", help="file path; stdout by default")
    args = parser.parse_args()
    cfg = Config.from_env()
    os.umask(0o077)  # session file = full account access; DB = third-party personal data

    if args.cmd == "export":
        store = Store(os.path.join(cfg.data_dir, "max.db"), outbox=False)
        out = open(args.out, "w", encoding="utf-8") if args.out else sys.stdout
        n = 0
        for n, row in enumerate(store.export(args.chat, to_ms(args.start), to_ms(args.end, end=True)), 1):
            out.write(json.dumps(row, ensure_ascii=False) + "\n")
        out.close() if args.out else out.flush()
        print(f"exported {n} messages", file=sys.stderr)
        return

    setup_logging()
    try:
        asyncio.run(Service(cfg).run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
