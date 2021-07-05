import copy
import json
import subprocess
import tempfile

import conbench.runner

from benchmarks import _benchmark


COMMON_OPTIONS = {
    "src": {
        "default": None,
        "type": str,
        "help": "Specify Arrow source directory.",
    },
}


def get_run_command(filename, options):
    # TODO: cd into arrow src dir?
    src = options.get("src")  # noqa

    command = [
        "yarn",
        "perf",
        "--json",
        filename,
    ]

    return command


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
    options = copy.deepcopy(COMMON_OPTIONS)
    description = "Run the Arrow JavaScript micro benchmarks."
    flags = {"language": "JavaScript"}

    def run(self, **kwargs):
        with tempfile.NamedTemporaryFile(delete=False) as result_file:
            run_command = get_run_command(result_file.name, kwargs)
            self._execute(run_command)
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

    def _execute(self, command):
        # TODO: move to base class?
        try:
            print(command)
            subprocess.run(command, capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            print(e.stderr.decode("utf-8"))
            raise e
