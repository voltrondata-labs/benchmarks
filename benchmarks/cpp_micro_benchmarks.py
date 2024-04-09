import copy
import logging
import os
from typing import List

import conbenchlegacy.runner
from benchadapt.adapters import ArcheryAdapter
from benchadapt.log import log

from benchmarks import _benchmark

log.setLevel(logging.DEBUG)

OPTIONS = {
    "repetitions": {
        "default": 1,
        "type": int,
        "help": "Number of repetitions to tell the executable to run.",
    },
    "repetition-min-time": {
        "default": 0.05,
        "type": float,
        "help": "Minimum time to run iterations for one repetition of the benchmark.",
    },
    "src": {
        "default": None,
        "type": str,
        "help": "Specify Arrow source directory.",
    },
    "suite-filter": {
        "default": "^(?!arrow-acero-aggregate-benchmark).*$",
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
    "rev-or-path": {
        "default": None,
        "type": str,
        "help": "Git rev or path to already-built arrow. Default is ${ARROW_BUILD_DIR}/cpp "
        "unless that env var is undefined (then build from scratch instead).",
    },
}


def _get_cli_options(options: dict) -> List[str]:
    command_params = []
    for option in OPTIONS:
        if option == "rev-or-path":
            value = options.get("rev_or_path", None)
            if not value and os.getenv("ARROW_BUILD_DIR"):
                value = f"{os.getenv('ARROW_BUILD_DIR')}/cpp"
            if value:
                command_params.append(value)
        else:
            value = options.get(option.replace("-", "_"), None)
            if value:
                command_params.extend([f"--{option}", str(value)])
    return command_params


@conbenchlegacy.runner.register_benchmark
class RecordCppMicroBenchmarks(_benchmark.Benchmark):
    """Run the Arrow C++ micro benchmarks."""

    external = True
    name = "cpp-micro"
    options = copy.deepcopy(OPTIONS)
    description = "Run the Arrow C++ micro benchmarks."
    iterations = None  # the executable handles repetitions internally
    flags = {"language": "C++"}
    adapter = None

    def __init__(self):
        # first so `.conbench` attribute exists
        super().__init__()
        # populates arrow metadata like compiler flags from pyarrow
        tags, info, context = self._get_tags_info_context(case=None, extra_tags={})

        # benchalerts recommends supplying commit information through environment variables
        os.environ["CONBENCH_PROJECT_REPOSITORY"] = self.github_info["repository"]
        os.environ["CONBENCH_PROJECT_COMMIT"] = self.github_info["commit"]
        if self.github_info["pr_number"]:
            os.environ["CONBENCH_PROJECT_PR_NUMBER"] = str(
                self.github_info["pr_number"]
            )

        self.adapter = ArcheryAdapter(
            result_fields_override={
                # this version grabs hostname from the `.conbench` file
                "machine_info": self.conbench.machine_info,
            },
            result_fields_append={"tags": tags, "info": info, "context": context},
        )

    def run(self, **kwargs):
        run_reason = kwargs.get("run_reason")
        run_name = kwargs.get("run_name", f"{run_reason}: {self.github_info['commit']}")
        run_id = kwargs.get("run_id")

        self.adapter.result_fields_override.update(
            run_reason=run_reason, run_name=run_name, run_id=run_id
        )
        command_params = _get_cli_options(kwargs)

        # don't rerun if generator called more than once
        if not self.adapter.results:
            self.adapter.run(command_params)

        results = self.adapter.results

        if not os.environ.get("DRY_RUN"):
            self.adapter.post_results()

        for res in results:
            res_json = res.to_publishable_dict()
            yield res_json, None
