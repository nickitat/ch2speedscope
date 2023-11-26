import ast
import subprocess

from idata_source_adaptor import IDataSourceAdaptor
from typing import List, Tuple


class ClickHouseTraceLogAdaptor(IDataSourceAdaptor):
    def __init__(
        self,
        query_id,
        host,
        port,
        user,
        pwd,
        secure,
        on_cluster,
        trace_types,
        sample_period,
    ):
        IDataSourceAdaptor.__init__(
            self, query_id, host, port, user, pwd, secure, on_cluster
        )
        self.trace_types = trace_types
        self.sample_period = sample_period

        self._check_revision()
        self._flush_logs()

    def get_value_unit(self) -> str:
        return "milliseconds"

    def get_start_value(self) -> int:
        query = f"SELECT {self._to_milliseconds('MIN(event_time_microseconds)')} FROM {self._table('system.trace_log')} WHERE query_id LIKE '{self.query_id}'"
        return ast.literal_eval(self._run_query(query))

    def get_end_value(self) -> int:
        query = f"SELECT {self._to_milliseconds('MAX(event_time_microseconds)')} FROM {self._table('system.trace_log')} WHERE query_id LIKE '{self.query_id}'"
        return ast.literal_eval(self._run_query(query))

    def get_frames(self) -> List:
        query = f"SELECT arrayReduce('groupUniqArray', arrayFlatten(groupUniqArray(trace))) FROM {self._table('system.trace_log')} WHERE query_id LIKE '{self.query_id}'"
        return ast.literal_eval(self._run_query(query))

    def get_demangled_frames(self, frames) -> List[str]:
        query = f"SELECT arrayMap(x -> demangle(addressToSymbol(x)), {frames})"
        return ast.literal_eval(self._run_query(query))

    def get_threads(self) -> List[Tuple[int | str, int | str]]:
        # first let's check if we have thread names
        query = f"SELECT DISTINCT (thread_id, thread_name) AS id_to_name FROM {self._table('system.query_thread_log')} WHERE query_id LIKE '{self.query_id}' FORMAT JSONEachRow"
        raw = [
            ast.literal_eval(elem)["id_to_name"]
            for elem in self._run_query(query).split("\n")[:-1]
        ]
        if len(raw):
            return [(elem[0], elem[1]) for elem in raw]

        # no luck, will use thread id as its name
        query = f"SELECT arrayReduce('groupUniqArray', arrayFlatten(groupUniqArray(thread_ids))) FROM {self._table('system.query_log')} WHERE query_id LIKE '{self.query_id}'"
        res = ast.literal_eval(self._run_query(query))
        return [(elem, elem) for elem in res]

    def get_events_for_thread(
        self, frames, thread_id, thread_name
    ) -> List[IDataSourceAdaptor.Event]:
        query = f"""
          SELECT arrayMap(elem -> arrayFirstIndex(x -> x = elem, {frames}), trace) AS frames, {self._to_milliseconds('event_time_microseconds')} AS at
            FROM {self._table('system.trace_log')}
           WHERE query_id LIKE '{self.query_id}' AND thread_id = {thread_id} AND trace_type IN {self.trace_types}
        ORDER BY at
          FORMAT TSV
        """
        events_raw = self._run_query(query).split("\n")[:-1]
        events_parsed = []
        for event in events_raw:
            pieces = event.split("\t")
            trace = [fr - 1 for fr in ast.literal_eval(pieces[0])]
            at = int(pieces[1])
            events_parsed.append(IDataSourceAdaptor.Event(list(reversed(trace)), at))
        return events_parsed

    def get_sample_period(self):
        if self.sample_period:
            return self.sample_period
        query = f"""
          WITH
               CAST(if(mapContains(Settings, 'query_profiler_real_time_period_ns'), Settings['query_profiler_real_time_period_ns'], '100000000'), 'UInt64') AS real,
               CAST(if(mapContains(Settings, 'query_profiler_cpu_time_period_ns'), Settings['query_profiler_cpu_time_period_ns'], '100000000'), 'UInt64') AS cpu
        SELECT intDiv(if(real < cpu, real, cpu), 1000000)
          FROM {self._table('system.query_log')}
         WHERE query_id LIKE '{self.query_id}' AND type = 'QueryStart'
        """
        return ast.literal_eval(self._run_query(query))

    def _check_revision(self):
        query_revision = self._run_query(f"""
        SELECT revision
          FROM {self._table('system.query_log')}
         WHERE query_id LIKE '{self.query_id}' AND type = 'QueryStart'
        """)
        current_revision = self._run_query("SELECT revision()")
        assert query_revision == current_revision, "current ClickHouse revision is different from the revision at query time, demangled stacks will be wrong"

    def _flush_logs(self):
        if not self.on_cluster:
            query = "SYSTEM FLUSH LOGS"
        else:
            query = f"SYSTEM FLUSH LOGS ON CLUSTER {self.on_cluster}"
        self._run_query(query)

    def _run_query(self, query):
        params = [
            "clickhouse-client",
            "-q",
            query,
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--user",
            self.user,
            "--password",
            self.pwd,
            "--allow_introspection_functions",
            "1",
        ]
        if self.secure:
            params += ["--secure"]
        res = subprocess.run(params, stdout=subprocess.PIPE)
        out = res.stdout.decode()
        return out

    def _table(self, table):
        if not self.on_cluster:
            return table
        return f"clusterAllReplicas({self.on_cluster}, {table})"

    def _to_milliseconds(self, col):
        return f"({col}::Decimal64(6) * 1000)::UInt64"
