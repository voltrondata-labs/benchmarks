import conbench.runner

import pyarrow
import pyarrow.parquet as parquet

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DataframeToTableBenchmark(_benchmark.BenchmarkPythonR):
    """Convert a pandas dataframe to an arrow table."""

    name, r_name = "dataframe-to-table", "df_to_table"
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

    def run(self, source, **kwargs):
        language = kwargs.get("language", "Python").lower()
        for source in self.get_sources(source):
            tags = self.get_tags(kwargs, source)
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
            f"cpu_count={self.r_cpu_count(options)})"
        )
