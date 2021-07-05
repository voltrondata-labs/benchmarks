import json
import os
import pathlib
import tempfile

import conbench.runner

from benchmarks import _benchmark


def get_run_command(filename):
    return [
        "yarn",
        "perf",
        "--json",
        filename,
    ]


def _parse_benchmark_name(full_name):
    # TODO: parse?
    # Examples:
    #   "name": "name: 'origin', length: 1,000,000, type: Dictionary<Int8, Utf8>, test: eq, value: Seattle",
    #   "name": "Table.from",
    #   "name": "length: 1,000,000",
    return full_name


@conbench.runner.register_benchmark
class RecordJavaScriptMicroBenchmarks(_benchmark.Benchmark):
    """Run the Arrow JavaScript micro benchmarks."""

    external = True
    name = "js-micro"
    options = {
        "src": {
            "default": None,
            "type": str,
            "help": "Specify Arrow source directory.",
        },
    }
    description = "Run the Arrow JavaScript micro benchmarks."
    flags = {"language": "JavaScript"}
    iterations = None

    def run(self, **kwargs):
        src = kwargs.get("src")
        if src:
            os.chdir(pathlib.Path(src).joinpath("js"))

        with tempfile.NamedTemporaryFile(delete=False) as result_file:
            run_command = get_run_command(result_file.name, kwargs)
            self.execute_command(run_command)
            results = json.load(result_file)
            for result in results:
                yield self._record_result(result, kwargs)

    def _record_result(self, result, options):
        context = {"benchmark_language": "JavaScript"}
        name = _parse_benchmark_name(result["name"])
        tags = {"source": self.name}
        values = self._get_values(result)
        return self.record(
            values,
            tags,
            context,
            options,
            output=result,
            name=name,
        )

    def _get_values(self, result):
        return {
            "data": result["details"]["sampleResults"],
            "unit": "s",  # TODO: confirm benchmark units
            "times": [],  # TODO: execution times?
            "time_unit": "s",
        }
