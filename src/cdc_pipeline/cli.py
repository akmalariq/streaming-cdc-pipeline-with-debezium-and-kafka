"""Command line interface for the CDC pipeline."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

from cdc_pipeline.config import get_settings
from cdc_pipeline.consumer import CDCConsumer
from cdc_pipeline.producer import simulate
from cdc_pipeline.sink import ClickHouseSink
from cdc_pipeline.transforms import SPECS

REPO_ROOT = Path(__file__).resolve().parents[2]
CONNECTOR_PATH = REPO_ROOT / "infra" / "debezium" / "postgres-connector.json"


def _cmd_simulate(args: argparse.Namespace) -> int:
    settings = get_settings()
    produced = simulate(settings, count=args.count, seed=args.seed)
    payload = {"simulated_events": produced, "topics": list(settings.source_topics)}
    print(json.dumps(payload, indent=2))
    return 0


def _cmd_consume(args: argparse.Namespace) -> int:
    overrides: dict = {}
    if args.batch_size:
        overrides["batch_size"] = args.batch_size
    if args.idle_timeout is not None:
        overrides["idle_timeout_seconds"] = args.idle_timeout
    settings = get_settings(**overrides)
    sink = ClickHouseSink(settings)
    consumer = CDCConsumer(settings, sink)
    metrics = consumer.run(max_messages=args.max_messages)
    report_path = metrics.write(Path(settings.data_dir))
    print(
        json.dumps(
            {
                "run_id": metrics.run_id,
                "processed": metrics.processed,
                "written": metrics.written,
                "decode_errors": metrics.decode_errors,
                "dlq_routed": metrics.dlq_routed,
                "max_lag": metrics.max_lag,
                "report": str(report_path),
            },
            indent=2,
        )
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    settings = get_settings()
    sink = ClickHouseSink(settings)
    summary: dict[str, dict[str, int]] = {}
    for spec in SPECS.values():
        if args.optimize:
            sink.optimize(spec.target)
        summary[spec.target] = {
            "rows": sink.count(spec.target),
            "active": sink.active_count(spec.target),
        }
    print(json.dumps(summary, indent=2))
    return 0


def _cmd_register_connector(args: argparse.Namespace) -> int:
    settings = get_settings()
    if not CONNECTOR_PATH.exists():
        print(f"connector config not found: {CONNECTOR_PATH}", file=sys.stderr)
        return 1

    payload = json.loads(CONNECTOR_PATH.read_text(encoding="utf-8"))
    url = f"{settings.connect_url}/connectors"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8")
            print(json.dumps({"status": response.status, "response": json.loads(body)}, indent=2))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code == 409:
            print(json.dumps({"status": 409, "detail": "connector already exists"}, indent=2))
            return 0
        print(f"failed to register connector ({exc.code}): {detail}", file=sys.stderr)
        return 1
    except urllib.error.URLError as exc:
        print(f"cannot reach Kafka Connect at {settings.connect_url}: {exc}", file=sys.stderr)
        return 1
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cdc", description="Streaming CDC pipeline: Debezium, Kafka, and a ClickHouse sink."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    simulate_parser = subparsers.add_parser("simulate", help="produce simulated change events")
    simulate_parser.add_argument("--count", type=int, default=25)
    simulate_parser.add_argument("--seed", type=int, default=None)
    simulate_parser.set_defaults(func=_cmd_simulate)

    consume_parser = subparsers.add_parser("consume", help="consume events into ClickHouse")
    consume_parser.add_argument("--max-messages", type=int, default=None)
    consume_parser.add_argument("--batch-size", type=int, default=None)
    consume_parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="seconds without a message before a bounded run stops",
    )
    consume_parser.set_defaults(func=_cmd_consume)

    verify_parser = subparsers.add_parser("verify", help="report sink row counts")
    verify_parser.add_argument(
        "--optimize", action="store_true", help="OPTIMIZE FINAL before counting"
    )
    verify_parser.set_defaults(func=_cmd_verify)

    connector_parser = subparsers.add_parser(
        "register-connector", help="POST the Debezium connector to Kafka Connect"
    )
    connector_parser.set_defaults(func=_cmd_register_connector)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    sys.exit(main())
