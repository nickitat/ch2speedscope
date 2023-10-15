import ast
import subprocess

from idata_source_adaptor import IDataSourceAdaptor
from typing import List, Tuple


def _run_query(query):
    res = subprocess.run(["clickhouse-client", "-q", query], stdout=subprocess.PIPE)
    out = res.stdout.decode()
    return out


def _to_milliseconds(col):
    return f"({col}::Decimal64(6) * 1000)::UInt64"


class ClickHouseTraceLogAdaptor(IDataSourceAdaptor):
    def get_value_unit(self) -> str:
        return "milliseconds"

    def get_start_value(self) -> int:
        query = f"SELECT {_to_milliseconds('MIN(event_time_microseconds)')} FROM system.trace_log WHERE query_id = '{self.query_id}'"
        return int(_run_query(query).strip())  # TODO

    def get_end_value(self) -> int:
        query = f"SELECT {_to_milliseconds('MAX(event_time_microseconds)')} FROM system.trace_log WHERE query_id = '{self.query_id}'"
        return int(_run_query(query).strip())  # TODO

    def get_frames(self) -> List:
        query = f"SELECT arrayReduce('groupUniqArray', arrayFlatten(groupUniqArray(trace))) FROM system.trace_log WHERE query_id = '{self.query_id}'"
        return ast.literal_eval(_run_query(query))

    def get_demangled_frames(self, frames) -> List[str]:
        query = f"SELECT arrayMap(x -> demangle(addressToSymbol(x)), {frames})"
        return ast.literal_eval(_run_query(query))

    def get_threads(self) -> List[Tuple[int | str, int | str]]:
        query = f"SELECT DISTINCT (thread_id, thread_name) AS id_to_name FROM system.query_thread_log WHERE query_id = '{self.query_id}' FORMAT JSONEachRow"
        raw = [
            ast.literal_eval(elem)["id_to_name"]
            for elem in _run_query(query).split("\n")[:-1]
        ]
        return [(elem[0], elem[1]) for elem in raw]

    def get_events_for_thread(
        self, frames, thread_id, thread_name
    ) -> List[IDataSourceAdaptor.Event]:
        query = f"SELECT arrayMap(elem -> arrayFirstIndex(x -> x = elem, {frames}), trace) AS frames, {_to_milliseconds('event_time_microseconds')} AS at FROM system.trace_log WHERE query_id = '{self.query_id}' AND thread_id = {thread_id} ORDER BY at FORMAT TSV"
        events_raw = _run_query(query).split("\n")[:-1]
        events_parsed = []
        for event in events_raw:
            pieces = event.split("\t")
            trace = [fr - 1 for fr in ast.literal_eval(pieces[0])]
            at = int(pieces[1])
            events_parsed.append(IDataSourceAdaptor.Event(list(reversed(trace)), at))
        return events_parsed
