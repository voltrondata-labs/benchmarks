import copy
import os
from typing import List

import conbench.runner
from benchadapt.adapters import ArcheryAdapter

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


def _get_cli_options(options: dict) -> List[str]:
    command_params = []
    if options.get("iterations"):
        command_params += ["--repetitions", str(options.get("iterations"))]

    _add_command_options(command_params, options)

    return command_params


def _add_command_options(command: List[str], options: dict):
    for option in COMMON_OPTIONS.keys():
        value = options.get(option.replace("-", "_"), None)
        if value:
            command.extend([f"--{option}", value])


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
    adapter = None

    def __init__(self):
        tags, info, context = self._get_tags_info_context(case=None, extra_tags={})

        self.adapter = ArcheryAdapter(
            result_fields_override={"github": self.github_info},
            result_fields_append={"tags": tags, "info": info, "context": context},
        )
        super().__init__()

    def run(self, **kwargs):
        run_reason = kwargs.get("run_reason")
        run_name = kwargs.get("run_name", f"{run_reason}: {self.github_info['commit']}")

        self.adapter.result_fields_override.update(
            run_reason=run_reason, run_name=run_name
        )
        command_params = _get_cli_options(kwargs)
        results = self.adapter.run(command_params)

        if not os.environ.get("DRY_RUN"):
            self.adapter.post_results()

        results_json = [res.to_publishable_dict() for res in results]
        return results_json, None
