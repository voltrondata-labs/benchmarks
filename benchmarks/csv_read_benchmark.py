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
                f = self._get_benchmark_function(source, *case)
                tags = self.get_tags(kwargs, source)
                yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, source, streaming, compression):
        # Note: this will write a comma separated csv with a header, even if
        # the original source file lacked a header and was pipe delimited.
        path = source.create_if_not_exists("csv", compression)
        munged = compression if compression != "uncompressed" else None
        schema = source.table.schema

        def read_streaming():
            table = pyarrow.csv.open_csv(
                pyarrow.input_stream(path, compression=munged),
                convert_options=pyarrow.csv.ConvertOptions(column_types=schema),
            ).read_all()
            return table

        def read_file():
            table = pyarrow.csv.read_csv(
                pyarrow.input_stream(path, compression=munged),
                convert_options=pyarrow.csv.ConvertOptions(column_types=schema),
            )
            return table

        return read_streaming if streaming == "streaming" else read_file
