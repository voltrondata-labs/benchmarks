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
        "2>",
        filename,
    ]


def _parse_benchmark_tags(name):
    keys, values, first = [], [], True
    for p in name.split(":"):
        parts = p.rsplit(",", 1)
        if len(parts) == 1:
            if first:
                keys.append(parts[0].strip())
                first = False
            else:
                values.append(parts[0].strip())
        if len(parts) == 2:
            keys.append(parts[1].strip())
            values.append(parts[0].strip())
    return dict(zip(keys, values))


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
        if not src:
            src = os.environ.get("ARROW_SRC")
        if src:
            os.chdir(pathlib.Path(src).joinpath("js"))

        with tempfile.NamedTemporaryFile(delete=False) as result_file:
            run_command = get_run_command(result_file.name)
            self.execute_command(run_command)
            results = json.load(result_file)
            for result in results:
                yield self._record_result(result, kwargs)

    def _record_result(self, result, options):
        context = {"benchmark_language": "JavaScript"}
        tags = _parse_benchmark_tags(result["name"])
        tags["source"] = self.name
        values = self._get_values(result)
        return self.record(
            values,
            tags,
            context,
            options,
            output=result,
            name=result["suite"],
        )

    def _get_values(self, result):
        return {
            "data": result["details"]["sampleResults"],
            "unit": "s",  # TODO: confirm benchmark units
            "times": [],  # TODO: execution times?
            "time_unit": "s",
        }
