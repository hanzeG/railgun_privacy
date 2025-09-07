#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Analyse a Railgun-like NDJSON dataset with fixed relative inputs and outputs.

USAGE:
    python analyse.py external test   # process ../data/_raw_test.ndjson
    python analyse.py external v2     # process ../data/_raw_v2.ndjson

Behaviour:
- Summarise transactions by 'to' address (Railgun:Relay, WETH Helper, OTHER).
- Print only totals and percentage breakdown (no per-transaction details).
- Additionally, collect for OTHER: (a) unique 'to' addresses (list and count) and (b) transaction hashes in original order; write all stats to ../data/external_<mode>.json.
All code comments and strings are in English.

Refactor notes:
- The script is refactored into a class `RailgunAnalyser` to ease future extension while preserving current behaviour and outputs.
- Public methods intentionally small to support adding new analysis passes (e.g., summarise_by_from()).
"""

import argparse
import json
import os
import sys
from typing import Any, Dict, Generator, Iterable, List, Optional

# Resolve base and data directories relative to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", "data"))

# Fixed dataset paths (relative)
TEST_INPUT_PATH = os.path.join(DATA_DIR, "_raw_test.ndjson")
V2_INPUT_PATH = os.path.join(DATA_DIR, "_raw_v2.ndjson")

# Canonical addresses (lowercased)
ADDR_RAILGUN_RELAY = "0xfa7093cdd9ee6932b4eb2c9e1cde7ce00b1fa4b9"
ADDR_WETH_HELPER = "0x4025ee6512dbbda97049bcf5aa5d38c54af6be8a"
ADDR_RAILGUN_TREASURE = "0xe8a8b458bcd1ececc6b6b58f80929b29ccecff40"

# Human-readable names
NAME_RAILGUN_RELAY = "Railgun:Relay"
NAME_WETH_HELPER = "WETH Helper"
NAME_RAILGUN_TREASURE = "Railgun:Treasure"

# Replacement maps (kept for potential future use)
TOPLEVEL_MAP = {
    # Only replace Relay and WETH Helper at top-level from/to
    ADDR_RAILGUN_RELAY: NAME_RAILGUN_RELAY,
    ADDR_WETH_HELPER: NAME_WETH_HELPER,
}
THREE_ADDR_MAP = {
    # Replace Relay, WETH Helper, and Railgun:Treasure where requested
    ADDR_RAILGUN_RELAY: NAME_RAILGUN_RELAY,
    ADDR_WETH_HELPER: NAME_WETH_HELPER,
    ADDR_RAILGUN_TREASURE: NAME_RAILGUN_TREASURE,
}


class RailgunAnalyser:
    """Encapsulates dataset analysis with extendable passes."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.input_path = self._resolve_input_path(mode)
        self.external_json_path = os.path.join(DATA_DIR, f"external_{mode}.json")

        # Primary results for the current implemented pass (by 'to' address)
        self.count_relay: int = 0
        self.count_weth_helper: int = 0
        self.count_other: int = 0

        # Accumulators for OTHER details
        self.other_tx_hashes: List[str] = []
        self.seen_lower_to_original: Dict[str, str] = {}
        self.other_to_counts: Dict[str, int] = {}

        # Summary object (populated after analyse())
        self.summary: Optional[Dict[str, Any]] = None

    # ------------------------
    # Utility helpers
    # ------------------------
    @staticmethod
    def to_lower_addr(addr: Any) -> str:
        """Return lowercase address string if possible, else empty string."""
        if isinstance(addr, str):
            return addr.lower()
        return ""

    @staticmethod
    def replace_from_map(val: Any, mapping: Dict[str, str]) -> Any:
        """Replace address string using provided mapping; return original if not matched."""
        if isinstance(val, str):
            low = val.lower()
            if low in mapping:
                return mapping[low]
        return val

    @staticmethod
    def classify_to_address(to_addr: Any) -> str:
        """Classify by destination 'to' address."""
        low = RailgunAnalyser.to_lower_addr(to_addr)
        if low == ADDR_RAILGUN_RELAY:
            return "RELAY"
        if low == ADDR_WETH_HELPER:
            return "WETH_HELPER"
        return "OTHER"

    @staticmethod
    def _resolve_input_path(mode: str) -> str:
        """Return the fixed input path for the given mode ('test' or 'v2')."""
        if mode == "test":
            return TEST_INPUT_PATH
        if mode == "v2":
            return V2_INPUT_PATH
        raise ValueError("Unsupported mode")

    def _iterate_records(self) -> Generator[Dict[str, Any], None, None]:
        """Yield parsed JSON records from the input NDJSON file."""
        if not os.path.exists(self.input_path):
            print(f"Input file not found: {self.input_path}", file=sys.stderr)
            return
        with open(self.input_path, "r", encoding="utf-8") as fin:
            for line in fin:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    if isinstance(rec, dict):
                        yield rec
                except json.JSONDecodeError as e:
                    print(f"[WARN] Skip invalid JSON line: {e}", file=sys.stderr)
                    continue

    # ------------------------
    # Analysis passes
    # ------------------------
    def summarise_by_to(self, records: Iterable[Dict[str, Any]]) -> None:
        """Populate counters and accumulators for the 'by to' summary."""
        for rec in records:
            to_addr = rec.get("to")
            cat = self.classify_to_address(to_addr)
            if cat == "RELAY":
                self.count_relay += 1
            elif cat == "WETH_HELPER":
                self.count_weth_helper += 1
            else:
                self.count_other += 1
                txh = rec.get("transactionHash")
                if isinstance(txh, str):
                    self.other_tx_hashes.append(txh)
                addr = rec.get("to")
                if isinstance(addr, str):
                    low = addr.lower()
                    if low not in self.seen_lower_to_original:
                        self.seen_lower_to_original[low] = addr
                    self.other_to_counts[low] = self.other_to_counts.get(low, 0) + 1

    # ------------------------
    # Orchestration and output
    # ------------------------
    def analyse(self) -> Dict[str, Any]:
        """Run the current set of analysis passes and return the summary dict."""
        # Ensure output directory exists
        os.makedirs(DATA_DIR, exist_ok=True)

        # Run passes
        self.summarise_by_to(self._iterate_records())

        # Prepare OTHER unique 'to' addresses in order of first appearance
        other_to_unique = [self.seen_lower_to_original[k] for k in self.seen_lower_to_original.keys()]
        # Build frequency list preserving the same order
        other_to_frequencies = [
            {"address": addr, "count": self.other_to_counts.get(addr.lower(), 0)}
            for addr in other_to_unique
        ]

        total = self.count_relay + self.count_weth_helper + self.count_other

        def pct_value(n: int, d: int) -> float:
            return round((n / d * 100.0), 2) if d else 0.0

        self.summary = {
            "mode": self.mode,
            "input": {
                "path": self.input_path,
                "total_transactions": total,
            },
            "by_to": {
                "relay": {
                    "address": ADDR_RAILGUN_RELAY,
                    "name": NAME_RAILGUN_RELAY,
                    "count": self.count_relay,
                    "percentage": pct_value(self.count_relay, total),
                },
                "weth_helper": {
                    "address": ADDR_WETH_HELPER,
                    "name": NAME_WETH_HELPER,
                    "count": self.count_weth_helper,
                    "percentage": pct_value(self.count_weth_helper, total),
                },
                "other": {
                    "count": self.count_other,
                    "percentage": pct_value(self.count_other, total),
                    "to_addresses": {
                        "unique_count": len(other_to_unique),
                        "unique": other_to_unique,
                        "frequencies": other_to_frequencies,
                    },
                    "transaction_hashes": self.other_tx_hashes,
                },
            },
        }
        return self.summary

    def write_summary(self) -> None:
        """Write the current summary to the external JSON path."""
        if self.summary is None:
            # If analyse() has not been run yet, run it now.
            self.analyse()
        assert self.summary is not None
        with open(self.external_json_path, "w", encoding="utf-8") as fout_json:
            json.dump(self.summary, fout_json, ensure_ascii=False, indent=2)

    def print_summary(self) -> None:
        """Print a concise summary to stdout."""
        if self.summary is None:
            # If analyse() has not been run yet, run it now.
            self.analyse()
        assert self.summary is not None

        total = int(self.summary.get("input", {}).get("total_transactions", 0))
        def pct(n: int, d: int) -> str:
            return f"{(n / d * 100):.2f}%" if d else "0.00%"

        print("\n=== Summary ===")
        print(f"Total transactions: {total}")
        print(f"Railgun:Relay (to == {ADDR_RAILGUN_RELAY}): {self.count_relay} ({pct(self.count_relay, total)})")
        print(f"WETH Helper  (to == {ADDR_WETH_HELPER}): {self.count_weth_helper} ({pct(self.count_weth_helper, total)})")
        other_unique_count = (
            self.summary.get("by_to", {})
            .get("other", {})
            .get("to_addresses", {})
            .get("unique_count", 0)
        )
        print(f"OTHER unique 'to' addresses: {other_unique_count}")
        print(f"Summary JSON written to: {self.external_json_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Summarise Railgun-style NDJSON. Use subcommands to select actions."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 'external' subcommand generates external summary JSON and prints console summary
    p_external = subparsers.add_parser(
        "external",
        help="Generate external_<mode>.json and print a concise summary",
    )
    p_external.add_argument(
        "mode",
        choices=["test", "v2"],
        help="Dataset mode to process",
    )

    args = parser.parse_args()

    if args.command == "external":
        analyser = RailgunAnalyser(args.mode)
        if not os.path.exists(analyser.input_path):
            print(f"Input file not found: {analyser.input_path}", file=sys.stderr)
            sys.exit(1)
        analyser.analyse()
        analyser.write_summary()
        analyser.print_summary()
        return

    # Fallback (should not reach due to required=True)
    parser.print_help()
    sys.exit(2)


if __name__ == "__main__":
    main()