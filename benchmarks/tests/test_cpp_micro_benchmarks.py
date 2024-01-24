import copy

import pytest

from .. import cpp_micro_benchmarks
from ..tests import _asserts

HELP = """
Usage: conbench cpp-micro [OPTIONS]

  Run the Arrow C++ micro benchmarks.

Options:
  --src TEXT                   Specify Arrow source directory.
  --suite-filter TEXT          Regex filtering benchmark suites.
  --benchmark-filter TEXT      Regex filtering benchmarks.
  --cmake-extras TEXT          Extra flags/options to pass to cmake
                               invocation.
  --cc TEXT                    C compiler.
  --cxx TEXT                   C++ compiler.
  --cxx-flags TEXT             C++ compiler flags.
  --cpp-package-prefix TEXT    Value to pass for ARROW_PACKAGE_PREFIX and use
                               ARROW_DEPENDENCY_SOURCE=SYSTEM.
  --repetitions INTEGER        Number of repetitions to tell the executable to
                               run.  [default: 20]
  --repetition-min-time FLOAT  Minimum time to run iterations for one
                               repetition of the benchmark.  [default: 0.05]
  --show-result BOOLEAN        [default: true]
  --show-output BOOLEAN        [default: false]
  --run-id TEXT                Group executions together with a run id.
  --run-name TEXT              Free-text name of run (commit ABC, pull request
                               123, etc).
  --run-reason TEXT            Low-cardinality reason for run (commit, pull
                               request, manual, etc).
  --help                       Show this message and exit.
"""


def test_get_run_command():
    options = {
        "iterations": 100,
        "suite_filter": "arrow-compute-vector-selection-benchmark",
        "benchmark_filter": "TakeStringRandomIndicesWithNulls/262144/2",
    }
    actual = cpp_micro_benchmarks._get_cli_options(options)
    assert actual == [
        "--repetitions",
        "100",
        "--suite-filter",
        "arrow-compute-vector-selection-benchmark",
        "--benchmark-filter",
        "TakeStringRandomIndicesWithNulls/262144/2",
    ]


def assert_benchmark(result):
    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged, language="C++")
    assert munged["tags"] == {
        "name": "TakeStringRandomIndicesWithNulls",
        "params": "262144/1000",
        "source": "cpp-micro",
        "suite": "arrow-compute-vector-selection-benchmark",
    }
    assert munged["stats"]["unit"] == "i/s"
    assert munged["stats"]["time_unit"] == "ns"
    assert len(munged["stats"]["data"]) == 1
    assert len(munged["stats"]["times"]) == 1


@pytest.mark.slow
def test_cpp_micro():
    benchmark = cpp_micro_benchmarks.RecordCppMicroBenchmarks()
    suite_filter = "arrow-compute-vector-selection-benchmark"
    benchmark_filter = "TakeStringRandomIndicesWithNulls/262144/1000"
    [(result, output)] = benchmark.run(
        suite_filter=suite_filter,
        benchmark_filter=benchmark_filter,
        iterations=1,
    )
    assert_benchmark(result)


def test_cpp_micro_cli():
    command = ["conbench", "cpp-micro", "--help"]
    _asserts.assert_cli(command, HELP)
