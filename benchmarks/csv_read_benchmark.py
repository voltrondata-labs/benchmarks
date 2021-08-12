import conbench.runner
import pyarrow.csv

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class CsvReadBenchmark(_benchmark.Benchmark):
    """Read CSV file to arrow table."""

    name = "csv-read"
    arguments = ["source"]
    sources_test = ["fanniemae_2016Q4", "nyctaxi_2010-01"]
    sources = ["fanniemae_sample", "nyctaxi_sample"]
    valid_cases = [("streaming", "compression")] + [
        # ("streaming", "uncompressed"),
        ("file", "uncompressed"),
        # ("streaming", "gzip"),
        # ("file", "gzip"),
    ]

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            for case in cases:
                (streaming, compression) = case
                schema = source.table.schema
                path = source.create_if_not_exists("csv", compression)
                convert_options = pyarrow.csv.ConvertOptions(column_types=schema)
                munged = compression if compression != "uncompressed" else None
                f = self._get_benchmark_function(
                    source,
                    streaming,
                    path,
                    munged,
                    convert_options,
                )
                tags = self.get_tags(kwargs, source)
                yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, source, streaming, path, munged, convert_options):
        def read_csv_streaming():
            reader = pyarrow.csv.open_csv(
                pyarrow.input_stream(path, compression=munged),
                convert_options=convert_options,
                parse_options=source.csv_parse_options,
                read_options=source.csv_read_options,
            )
            return reader.read_all()

        def read_csv_file():
            table = pyarrow.csv.read_csv(
                pyarrow.input_stream(path, compression=munged),
                parse_options=source.csv_parse_options,
                read_options=source.csv_read_options,
            )
            return table

        return read_csv_streaming if streaming == "streaming" else read_csv_file
