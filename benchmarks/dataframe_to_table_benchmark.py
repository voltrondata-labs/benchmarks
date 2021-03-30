import conbench.runner

import pyarrow
import pyarrow.parquet as parquet

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DataframeToTableBenchmark(_benchmark.Benchmark, _benchmark.BenchmarkR):
    """
    Convert a pandas dataframe to an arrow table.

        DataframeToTableBenchmark().run(<source>, options...)

    Parameters
    ----------
    source : str
        A source name from the benchmarks source store.
    language : str, optional
        Valid values: "Python", "R".
    cpu_count : int, optional
        Set the number of threads to use in parallel operations (arrow).
    iterations : int, default 1
        Number of times to run the benchmark.
    gc_collect : boolean, default True
        Whether to do garbage collection before each benchmark run.
    gc_disable : boolean, default True
        Whether to do disable collection during each benchmark run.
    run_id : str, optional
        Group executions together with a run id.
    run_name : str, optional
        Name of run (commit, pull request, etc).

    Returns
    -------
    (result, output) : sequence
        result : The benchmark result.
        output : The output from the benchmarked function.
    """

    name, r_name = "dataframe-to-table", "df_to_table"
    arguments = ["source"]
    sources = [
        "chi_traffic_2020_Q1",
        "type_strings",
        "type_dict",
        "type_integers",
        "type_floats",
        "type_nested",
        "type_simple_features",
    ]
    sources_test = [
        "chi_traffic_sample",
        "type_strings",
        "type_dict",
        "type_integers",
        "type_floats",
        "type_nested",
        "type_simple_features",
    ]
    options = {
        "language": {"type": str, "choices": ["Python", "R"]},
        "cpu_count": {"type": int},
    }

    def run(self, source, cpu_count=None, **kwargs):
        language = kwargs.get("language", "Python").lower()
        for source in self.get_sources(source):
            tags = self.get_tags(source, cpu_count)
            if language == "python":
                dataframe = self._get_dataframe(source.source_path)
                f = self._get_benchmark_function(dataframe)
                yield self.benchmark(f, tags, kwargs)
            elif language == "r":
                command = self._get_r_command(source, kwargs)
                yield self.r_benchmark(command, tags, kwargs)

    def _get_benchmark_function(self, dataframe):
        return lambda: pyarrow.Table.from_pandas(dataframe)

    def _get_dataframe(self, path):
        return parquet.read_table(path, memory_map=False).to_pandas()

    def _get_r_command(self, source, options):
        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f'source="{source.name}", '
            f'n_iter={options.get("iterations", 1)}, '
            f"cpu_count={self.r_cpu_count(options)})"
        )
