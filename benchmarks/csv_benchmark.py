import itertools
from typing import Callable

import conbench.runner
import pyarrow.csv

from benchmarks import _benchmark, _sources


class CsvBenchmark(_benchmark.BenchmarkPythonR):
    """Parent class for CSV reading/writing benchmarks"""

    arguments = ["source"]
    sources = ["fanniemae_2016Q4", "nyctaxi_2010-01"]
    sources_test = ["fanniemae_sample", "nyctaxi_sample"]

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

    def _get_r_command(self, source, case, options) -> str:
        params = self._case_to_param_dict(case=case)

        if self.name == "csv-read":
            # TODO: remove once compression available for writing
            compression = f"compression=\"{params['compression']}\", "
            io_type = "output_format"
        elif self.name == "csv-write":
            compression = ""
            io_type = "input"

        r_command = f"""
            library(arrowbench);
            run_one({self.r_name},
            cpu_count={self.r_cpu_count(options)},
            source="{source.name}",
            {self.r_name[:4]}er='arrow',
            {compression}
            {io_type}=\"{params[io_type]}\")
        """

        return r_command

    def _case_to_param_dict(self, case: tuple) -> dict:
        params = {
            parameter: argument
            for parameter, argument in zip(self.valid_cases[0], case)
        }

        return params


@conbench.runner.register_benchmark
class CsvReadBenchmark(CsvBenchmark):
    """Read CSV file."""

    name = "csv-read"
    r_name = "read_csv"

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
        ("streaming", "compression", "output_format"),
        *sorted({*valid_python_cases, *valid_r_cases}),
    ]

    def _get_benchmark_function(
        self, source, streaming, compression, output_format
    ) -> Callable:
        # Note: this will write a comma separated csv with a header, even if
        # the original source file lacked a header and was pipe delimited.
        path = source.create_if_not_exists("csv", compression)
        munged = _sources.munge_compression(compression, file_type="csv")

        def read_streaming():
            table = pyarrow.csv.open_csv(
                pyarrow.input_stream(path, compression=munged),
                convert_options=source.csv_convert_options,
            ).read_all()
            return table

        def read_file():
            table = pyarrow.csv.read_csv(
                pyarrow.input_stream(path, compression=munged),
                convert_options=source.csv_convert_options,
            )
            return table

        return read_streaming if streaming == "streaming" else read_file


@conbench.runner.register_benchmark
class CsvWriteBenchmark(CsvBenchmark):
    """Write CSV file."""

    name = "csv-write"
    r_name = "write_csv"

    valid_python_cases = list(
        itertools.product(
            ["streaming", "file"], ["uncompressed", "gzip"], ["arrow_table"]
        )
    )
    valid_r_cases = list(
        itertools.product(["file"], ["uncompressed"], ["arrow_table", "data_frame"])
    )
    valid_cases = [
        ("streaming", "compression", "input"),
        *sorted({*valid_python_cases, *valid_r_cases}),
    ]

    def _get_benchmark_function(
        self, source, streaming, compression, input
    ) -> Callable:
        # Note: this will write a comma separated csv with a header, even if
        # the original source file lacked a header and was pipe delimited.
        compression = _sources.munge_compression(compression, file_type="csv")
        path = source.temp_path("csv", compression)
        schema = source.table.schema
        data = source.table
        out_stream = pyarrow.output_stream(path, compression=compression)

        def write_streaming():
            with pyarrow.csv.CSVWriter(out_stream, schema) as writer:
                writer.write_table(data)

        def write_file():
            pyarrow.csv.write_csv(data, out_stream)

        return write_streaming if streaming == "streaming" else write_file
