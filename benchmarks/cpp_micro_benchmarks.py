import copy
import json
import os
import tempfile

import conbench.runner

from benchmarks import _benchmark

RUN_OPTIONS = {
    "iterations": {
        "default": None,
        "type": int,
        "help": "Number of iterations of each benchmark.",
    },
}


COMMON_OPTIONS = {
    "src": {
        "default": None,
        "type": str,
        "help": "Specify Arrow source directory.",
    },
    "suite-filter": {
        "default": None,
        "type": str,
        "help": "Regex filtering benchmark suites.",
    },
    "benchmark-filter": {
        "default": None,
        "type": str,
        "help": "Regex filtering benchmarks.",
    },
    "cmake-extras": {
        "default": None,
        "type": str,
        "help": "Extra flags/options to pass to cmake invocation.",
    },
    "cc": {
        "default": None,
        "type": str,
        "help": "C compiler.",
    },
    "cxx": {
        "default": None,
        "type": str,
        "help": "C++ compiler.",
    },
    "cxx-flags": {
        "default": None,
        "type": str,
        "help": "C++ compiler flags.",
    },
    "cpp-package-prefix": {
        "default": None,
        "type": str,
        "help": "Value to pass for ARROW_PACKAGE_PREFIX and use ARROW_DEPENDENCY_SOURCE=SYSTEM.",
    },
}


def get_run_command(filename, options):
    command = [
        "archery",
        "benchmark",
        "run",
        "--output",
        filename,
    ]

    iterations = options.get("iterations", None)
    if iterations:
        command.extend(["--repetitions", str(iterations)])

    _add_command_options(command, options)
    return command


def _add_command_options(command, options):
    for option in COMMON_OPTIONS.keys():
        value = options.get(option.replace("-", "_"), None)
        if value:
            command.extend([f"--{option}", value])


def _parse_benchmark_name(full_name):
    parts = full_name.split("/", 1)
    name, params = parts[0], ""
    if len(parts) == 2:
        params = parts[1]

    parts = name.split("<", 1)
    if len(parts) == 2:
        if params:
            name, params = parts[0], f"<{parts[1]}/{params}"
        else:
            name, params = parts[0], f"<{parts[1]}"

    tags = {"name": name}
    if params:
        tags["params"] = params

    return tags


@conbench.runner.register_benchmark
class RecordCppMicroBenchmarks(_benchmark.Benchmark):
    """Run the Arrow C++ micro benchmarks."""

    external = True
    name = "cpp-micro"
    options = copy.deepcopy(COMMON_OPTIONS)
    options.update(**RUN_OPTIONS)
    description = "Run the Arrow C++ micro benchmarks."
    iterations = 1
    flags = {"language": "C++"}

    def run(self, **kwargs):
        with tempfile.NamedTemporaryFile(delete=False) as result_file:
            run_command = get_run_command(result_file.name, kwargs)
            self.execute_command(run_command)
            results = json.load(result_file)
            for suite in results["suites"]:
                self.conbench.mark_new_batch()
                for result in suite["benchmarks"]:
                    yield self._record_result(suite, result, kwargs)

    def _record_result(self, suite, result, options):
        info, context = {}, {"benchmark_language": "C++"}
        tags = _parse_benchmark_name(result["name"])
        name = tags.pop("name")
        tags["suite"] = suite["name"]
        tags["source"] = self.name

        values = self._get_values(result)

        return self.record(
            values,
            tags,
            info,
            context,
            options=options,
            output=result,
            name=name,
            publish=os.environ.get("DRY_RUN") is None,
        )

    def _get_values(self, result):
        return {
            "data": result["values"],
            "unit": self._format_unit(result["unit"]),
            "times": result.get("times", []),
            "time_unit": result.get("time_unit", "s"),
        }

    def _format_unit(self, x):
        if x == "bytes_per_second":
            return "B/s"
        if x == "items_per_second":
            return "i/s"
        return x
