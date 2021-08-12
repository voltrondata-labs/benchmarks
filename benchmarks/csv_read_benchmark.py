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
    valid_cases = [("streaming", "compression")] + [
        ("streaming", "uncompressed"),
        ("file", "uncompressed"),
        ("streaming", "gzip"),
        ("file", "gzip"),
    ]

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            for case in cases:
                (streaming, compression) = case
                schema = source.table.schema
                path = source.create_if_not_exists("csv", compression)
                munged = compression if compression != "uncompressed" else None
                f = self._get_benchmark_function(streaming, path, munged, schema)
                tags = self.get_tags(kwargs, source)
                yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, streaming, path, munged, schema):
        def read_csv_streaming():
            return pyarrow.csv.open_csv(
                pyarrow.input_stream(path, compression=munged),
                convert_options=pyarrow.csv.ConvertOptions(column_types=schema),
            ).read_all()

        def read_csv_file():
            return pyarrow.csv.read_csv(
                pyarrow.input_stream(path, compression=munged),
            )

        return read_csv_streaming if streaming == "streaming" else read_csv_file
