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
                schema = source.table.schema
                f = self._get_benchmark_function(source, schema, *case)
                tags = self.get_tags(kwargs, source)
                yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, source, schema, streaming, compression):
        path = source.create_if_not_exists("csv", compression)
        munged_compression = compression if compression != "uncompressed" else None
        in_stream = pyarrow.input_stream(path, compression=munged_compression)
        convert_options = pyarrow.csv.ConvertOptions(column_types=schema)

        def read_csv_streaming():
            reader = pyarrow.csv.open_csv(
                in_stream,
                convert_options=convert_options,
                parse_options=source.csv_parse_options,
                read_options=source.csv_read_options,
            )
            return reader.read_all()

        def read_csv_file():
            table = pyarrow.csv.read_csv(
                in_stream,
                parse_options=source.csv_parse_options,
                read_options=source.csv_read_options,
            )
            return table

        return read_csv_streaming if streaming == "streaming" else read_csv_file
