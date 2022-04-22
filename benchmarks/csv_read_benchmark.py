import itertools

import conbench.runner
import pyarrow.csv

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class CsvReadBenchmark(_benchmark.BenchmarkPythonR):
    """Read CSV file."""

    name = "csv-read"
    r_name = "read_csv"
    arguments = ["source"]
    sources = ["fanniemae_2016Q4", "nyctaxi_2010-01"]
    sources_test = ["fanniemae_sample", "nyctaxi_sample"]
    valid_python_cases = list(
        itertools.product(
            ["streaming", "file"], ["uncompressed", "gzip"], ["arrow_table"]
        )
    )
    valid_r_cases = list(
        itertools.product(
            ["file"], ["uncompressed", "gzip"], ["arrow_table", "data_frame"]
        )
    )
    valid_cases = [
        ("streaming", "compression", "output"),
        *sorted({*valid_python_cases, *valid_r_cases}),
    ]

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        language = kwargs.get("language", "Python").lower()

        for source in self.get_sources(source):
            tags = self.get_tags(kwargs, source)

            for case in cases:
                if language == "python":
                    if case not in self.valid_python_cases:
                        continue
                    f = self._get_benchmark_function(source, *case)
                    yield self.benchmark(f, tags, kwargs, case)
                elif language == "r":
                    if case not in self.valid_r_cases:
                        continue
                    command = self._get_r_command(source, case, kwargs)
                    yield self.r_benchmark(
                        command=command, extra_tags=tags, options=kwargs, case=case
                    )

    def _get_benchmark_function(self, source, streaming, compression, output):
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

    def _get_r_command(self, source, case, options):
        params = self._case_to_param_dict(case=case)

        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)}, "
            f'source="{source.name}", '
            f"reader='arrow', "
            f"compression=\"{params['compression']}\", "
            f"output=\"{params['output']}\")"
        )

    def _case_to_param_dict(self, case):
        params = {
            parameter: argument
            for parameter, argument in zip(self.valid_cases[0], case)
        }

        return params
