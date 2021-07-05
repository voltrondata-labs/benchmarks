import copy
import json
import subprocess
import tempfile

import conbench.runner

from benchmarks import _benchmark


RUN_OPTIONS = {
    "iterations": {
        "default": None,
        "type": int,
        "help": "Number of iterations of each benchmark.",
    },
    "commit": {
        "default": None,
        "type": str,
        "help": "Arrow commit.",
    },
}


COMMON_OPTIONS = {
    "src": {
        "default": None,
        "type": str,
        "help": "Specify Arrow source directory.",
    },
    "benchmark-filter": {
        "default": None,
        "type": str,
        "help": "Regex filtering benchmarks.",
    },
    "java-home": {
        "default": None,
        "type": str,
        "help": "Path to Java Developers Kit.",
    },
    "java-options": {
        "default": None,
        "type": str,
        "help": "Java compiler options.",
    },
}


def get_run_command(filename, options):
    commit = options.get("commit", "HEAD")
    command = [
        "archery",
        "benchmark",
        "run",
        commit,
        "--language",
        "java",
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
    suite, name = full_name.rsplit(".", 1)
    suite = suite.split("org.apache.", 1)[1]
    return suite, name


@conbench.runner.register_benchmark
class RecordJavaMicroBenchmarks(_benchmark.Benchmark):
    """Run the Arrow Java micro benchmarks."""

    external = True
    name = "java-micro"
    options = copy.deepcopy(COMMON_OPTIONS)
    options.update(**RUN_OPTIONS)
    description = "Run the Arrow Java micro benchmarks."
    flags = {"language": "Java"}
    iterations = 1

    def run(self, **kwargs):
        with tempfile.NamedTemporaryFile(delete=False) as result_file:
            run_command = get_run_command(result_file.name, kwargs)
            self._execute(run_command)
            results = json.load(result_file)

            # the java micro benchmarks are not bucketed by suite, bucket them
            suites = {}
            for suite in results["suites"]:
                for result in suite["benchmarks"]:
                    name, _ = _parse_benchmark_name(result["name"])
                    if name not in suites:
                        suites[name] = []
                    suites[name].append(result)

            for name in suites:
                self.conbench.mark_new_batch()
                for result in suites[name]:
                    yield self._record_result(result, kwargs)

    def _record_result(self, result, options):
        context = {"benchmark_language": "Java"}
        suite, name = _parse_benchmark_name(result["name"])
        tags = {"suite": suite, "source": self.name}
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
        # the java micro benchmarks do not contain the raw execution times
        return {
            "data": result["values"],
            "unit": self._format_unit(result["unit"]),
            "times": [],
            "time_unit": "s",
        }

    def _format_unit(self, x):
        if x == "items_per_second":
            return "i/s"
        return x

    def _execute(self, command):
        try:
            print(command)
            subprocess.run(command, capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            print(e.stderr.decode("utf-8"))
            raise e
