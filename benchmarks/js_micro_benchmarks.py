import json
import os
import pathlib

import conbench.runner

from benchmarks import _benchmark


def get_run_command():
    return ["yarn", "perf", "--json"]


def _parse_benchmark_tags(name):
    keys, values, first = [], [], True
    colons = name.split(":")
    num_colons = len(colons) - 1
    for x in colons:
        if first:
            keys.append(x.strip())
            first = False
        elif len(keys) == num_colons:
            values.append(x.strip())
        else:
            parts = x.rsplit(",", 1)
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

        run_command = get_run_command()
        _, err = self.execute_command(run_command)
        results = json.loads(err)

        # bucket by suite
        suites = {}
        for result in results:
            name = result["suite"]
            if name not in suites:
                suites[name] = []
            suites[name].append(result)

        for name in suites:
            self.conbench.mark_new_batch()
            for result in suites[name]:
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
            "unit": "s",
            "times": [],
            "time_unit": "s",
        }
