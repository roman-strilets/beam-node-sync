"""Command-line entry point for the Beam node sync prototype."""

from __future__ import annotations

import argparse
import sys

from src.protocol import DEFAULT_CONNECT_TIMEOUT, DEFAULT_PORT, DEFAULT_REQUEST_TIMEOUT
from src.storage import JsonLineWriter
from src.syncer import DeriveConfig, SyncConfig, run_derive, run_stage, run_sync
from src.utils import parse_endpoint, parse_fork_hashes


def main(argv: list[str] | None = None) -> int:
    """Parse command-line arguments and run the Beam sync/export flow."""
    parser = argparse.ArgumentParser(
        description=(
            "Synchronize Beam regular outputs from a trusted node, or stage raw "
            "blocks first and derive the UTXO set in a separate pass"
        )
    )
    parser.add_argument(
        "node",
        nargs="?",
        help="Beam node address as host or host:port (required for direct, stage, and staged modes)",
    )
    parser.add_argument(
        "--mode",
        choices=("direct", "stage", "derive", "staged"),
        default="direct",
        help=(
            "direct: fetch and derive in one pass; stage: persist raw headers and bodies only; "
            "derive: replay staged data into the UTXO state; staged: run stage then derive"
        ),
    )
    parser.add_argument(
        "--state-db",
        default="beam-sync.sqlite3",
        help="SQLite file used for resumable sync state (default: beam-sync.sqlite3)",
    )
    parser.add_argument(
        "--start-height",
        type=int,
        metavar="HEIGHT",
        help=(
            "request work starting at this block height; already-processed earlier "
            "heights are skipped, but local state still advances contiguously"
        ),
    )
    parser.add_argument(
        "--stop-height",
        type=int,
        metavar="HEIGHT",
        help=(
            "stop after staging or deriving this block height instead of using the "
            "current node tip or max staged height"
        ),
    )
    parser.add_argument(
        "--connect-timeout",
        type=float,
        default=DEFAULT_CONNECT_TIMEOUT,
        help=(
            "TCP connect and handshake timeout in seconds "
            f"(default {DEFAULT_CONNECT_TIMEOUT:g})"
        ),
    )
    parser.add_argument(
        "--request-timeout",
        type=float,
        default=DEFAULT_REQUEST_TIMEOUT,
        help=(
            "seconds to wait for each header/body response "
            f"(default {DEFAULT_REQUEST_TIMEOUT:g})"
        ),
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        metavar="COUNT",
        help="print a progress line every COUNT applied blocks (default: 1000)",
    )
    parser.add_argument(
        "--fast-sync",
        action="store_true",
        help=(
            "use Beam-style sparse historical body fetches when syncing a contiguous "
            "range from the current local state"
        ),
    )
    parser.add_argument(
        "--fork-hash",
        action="append",
        default=[],
        metavar="HEX",
        help="fork config hash (64 hex chars); repeat per fork",
    )
    parser.add_argument(
        "-o",
        "--output",
        help="write exported UTXOs to this JSONL file instead of stdout",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="print handshake and sync diagnostics to stderr",
    )
    args = parser.parse_args(argv)

    if args.connect_timeout <= 0:
        parser.error("connect-timeout must be > 0")
    if args.request_timeout <= 0:
        parser.error("request-timeout must be > 0")
    if args.progress_every <= 0:
        parser.error("progress-every must be > 0")
    if args.start_height is not None and args.start_height <= 0:
        parser.error("start-height must be > 0")
    if args.stop_height is not None and args.stop_height <= 0:
        parser.error("stop-height must be > 0")
    if (
        args.start_height is not None
        and args.stop_height is not None
        and args.start_height > args.stop_height
    ):
        parser.error("start-height must be <= stop-height")
    if args.mode in {"direct", "stage", "staged"} and args.node is None:
        parser.error("node is required for direct, stage, and staged modes")
    if args.mode == "stage" and args.output is not None:
        parser.error("--output is only valid for direct, derive, and staged modes")

    try:
        endpoint = parse_endpoint(args.node, DEFAULT_PORT) if args.node is not None else None
        fork_hashes = parse_fork_hashes(args.fork_hash)

        if args.mode == "direct":
            with JsonLineWriter(args.output) as writer:
                result = run_sync(
                    SyncConfig(
                        endpoint=endpoint,
                        state_db_path=args.state_db,
                        connect_timeout=args.connect_timeout,
                        request_timeout=args.request_timeout,
                        fork_hashes=fork_hashes,
                        start_height=args.start_height,
                        stop_height=args.stop_height,
                        progress_every=args.progress_every,
                        fast_sync=args.fast_sync,
                        verbose=args.verbose,
                    ),
                    writer,
                )

            print(
                f"synced height={result.synced_height}/{result.target_height} "
                f"from {result.node}; applied_blocks={result.applied_blocks}, "
                f"outputs={result.outputs_seen}, resolved_spends={result.resolved_spends}, "
                f"unresolved_spends={result.unresolved_spends}, exported_utxos={result.exported_utxos}, "
                f"elapsed={result.duration_seconds:.2f}s",
                file=sys.stderr,
            )
            return 0

        if args.mode == "stage":
            result = run_stage(
                SyncConfig(
                    endpoint=endpoint,
                    state_db_path=args.state_db,
                    connect_timeout=args.connect_timeout,
                    request_timeout=args.request_timeout,
                    fork_hashes=fork_hashes,
                    start_height=args.start_height,
                    stop_height=args.stop_height,
                    progress_every=args.progress_every,
                        fast_sync=args.fast_sync,
                    verbose=args.verbose,
                )
            )
            print(
                f"staged height={result.staged_height}/{result.target_height} "
                f"from {result.node}; staged_blocks={result.staged_blocks}, "
                f"elapsed={result.duration_seconds:.2f}s",
                file=sys.stderr,
            )
            return 0

        if args.mode == "derive":
            with JsonLineWriter(args.output) as writer:
                result = run_derive(
                    DeriveConfig(
                        state_db_path=args.state_db,
                        start_height=args.start_height,
                        stop_height=args.stop_height,
                        progress_every=args.progress_every,
                    ),
                    writer,
                )

            print(
                f"derived height={result.synced_height}/{result.target_height}; "
                f"applied_blocks={result.applied_blocks}, outputs={result.outputs_seen}, "
                f"resolved_spends={result.resolved_spends}, unresolved_spends={result.unresolved_spends}, "
                f"exported_utxos={result.exported_utxos}, elapsed={result.duration_seconds:.2f}s",
                file=sys.stderr,
            )
            return 0

        stage_result = run_stage(
            SyncConfig(
                endpoint=endpoint,
                state_db_path=args.state_db,
                connect_timeout=args.connect_timeout,
                request_timeout=args.request_timeout,
                fork_hashes=fork_hashes,
                start_height=args.start_height,
                stop_height=args.stop_height,
                progress_every=args.progress_every,
                fast_sync=args.fast_sync,
                verbose=args.verbose,
            )
        )
        with JsonLineWriter(args.output) as writer:
            derive_result = run_derive(
                DeriveConfig(
                    state_db_path=args.state_db,
                    start_height=args.start_height,
                    stop_height=args.stop_height,
                    progress_every=args.progress_every,
                ),
                writer,
            )

        print(
            f"staged height={stage_result.staged_height}/{stage_result.target_height} "
            f"from {stage_result.node}; staged_blocks={stage_result.staged_blocks}, "
            f"stage_elapsed={stage_result.duration_seconds:.2f}s",
            file=sys.stderr,
        )
        print(
            f"derived height={derive_result.synced_height}/{derive_result.target_height}; "
            f"applied_blocks={derive_result.applied_blocks}, outputs={derive_result.outputs_seen}, "
            f"resolved_spends={derive_result.resolved_spends}, "
            f"unresolved_spends={derive_result.unresolved_spends}, "
            f"exported_utxos={derive_result.exported_utxos}, "
            f"derive_elapsed={derive_result.duration_seconds:.2f}s",
            file=sys.stderr,
        )
        return 0
    except Exception as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
