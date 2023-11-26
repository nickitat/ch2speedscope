#!/usr/bin/env python3

import json
import os

from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
from source_trace_log import ClickHouseTraceLogAdaptor


def _create_opening_events(trace, at):
    events = []
    for fr in trace:
        events.append({"type": "O", "frame": fr, "at": at})
    return events


def _create_closing_events(trace, at):
    events = []
    for fr in reversed(trace):
        events.append({"type": "C", "frame": fr, "at": at})
    return events


def _parse_args():
    parser = ArgumentParser(
        description="ch2speedscope", formatter_class=ArgumentDefaultsHelpFormatter
    )

    parser.add_argument("-q", "--query_id", type=str)
    parser.add_argument(
        "--host",
        type=str,
        default="localhost",
        help="hostname of the ClickHouse cluster (single host or balancer)",
    )
    parser.add_argument("--port", type=int, default=9000, help="port")
    parser.add_argument("--user", type=str, default="default", help="user name")
    parser.add_argument("--pwd", "--password", type=str, default="", help="password")
    parser.add_argument(
        "--secure",
        action="store_true",
        default=False,
        help="pass --secure flag to ClickHouse client",
    )
    parser.add_argument(
        "--on_cluster",
        type=str,
        default="",
        help="query all system tables on the specified cluster if needed",
    )
    parser.add_argument(
        "--trace_types",
        type=str,
        nargs="+",
        default=["Real", "CPU"],
        choices=["Real", "CPU", "Memory", "MemorySample", "MemoryPeak"],
        help="choose trace types to select from system.trace_log",
    )
    parser.add_argument(
        "--sample_period",
        type=int,
        default=None,
        help="sample period in milliseconds. if no value provided, we will search for a value in `Settings` field of system.query_log. if no value will be found, the default value (100 ms) will be used",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    ds_adaptor = ClickHouseTraceLogAdaptor(**vars(args))

    sample_period_ms = ds_adaptor.get_sample_period()
    start = ds_adaptor.get_start_value()
    end = ds_adaptor.get_end_value()
    frames = ds_adaptor.get_frames()
    demangled_frames = ds_adaptor.get_demangled_frames(frames)
    threads = ds_adaptor.get_threads()

    profiles = []
    for thread, thread_name in threads:
        src_events = ds_adaptor.get_events_for_thread(frames, thread, thread_name)
        if not src_events:
            continue

        gen_events = _create_opening_events(src_events[0].trace, src_events[0].at)

        for i in range(1, len(src_events)):
            trace = src_events[i].trace
            at = src_events[i].at
            prev_trace = src_events[i - 1].trace
            prev_at = src_events[i - 1].at

            if at - prev_at <= sample_period_ms:
                # No events will be produced for the frames inherited from the previous trace
                common_prefix = len(os.path.commonprefix([prev_trace, trace]))
            else:
                common_prefix = 0

            closing_at = min(at, prev_at + sample_period_ms)

            # Close all the frames above the common prefix from the previous trace
            gen_events += _create_closing_events(prev_trace[common_prefix:], closing_at)

            # Open all the frames above the common prefix from the current trace
            gen_events += _create_opening_events(trace[common_prefix:], at)

        gen_events += _create_closing_events(
            src_events[-1].trace, src_events[-1].at + sample_period_ms
        )

        profiles.append(
            {
                "type": "evented",
                "name": f"{thread} ({thread_name})",
                "unit": f"{ds_adaptor.get_value_unit()}",
                "startValue": start,
                "endValue": end,
                "events": gen_events,
            }
        )

    report = {
        "version": "0.0.1",
        "$schema": "https://www.speedscope.app/file-format-schema.json",
        "shared": {
            "frames": [
                {"name": elem if elem else "unknown"} for elem in demangled_frames
            ]
        },
        "profiles": profiles,
    }

    print(json.dumps(report))
