import conbench.runner
import pyarrow.csv

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class CsvReadBenchmark(_benchmark.Benchmark):
    """Read CSV file to arrow table."""

    name = "csv-read"
    arguments = ["source"]
    sources = ["fanniemae_2016Q4", "nyctaxi_2010-01"]
    sources_test = ["fanniemae_sample", "nyctaxi_sample"]

    def run(self, source, **kwargs):
        for source in self.get_sources(source):
            f = self._get_benchmark_function(
                source.source_path,
                source.csv_parse_options,
            )
            tags = self.get_tags(kwargs, source)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, source_path, parse_options):
        return lambda: pyarrow.csv.read_csv(
            source_path,
            parse_options=parse_options,
        )
