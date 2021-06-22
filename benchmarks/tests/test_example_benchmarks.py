import copy

import pytest

from .. import _example_benchmarks
from ..tests._asserts import assert_context, assert_cli, R_CLI


SIMPLE_HELP = """
Usage: conbench example-simple [OPTIONS]

  Run example-simple benchmark.

Options:
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: False]
  --gc-collect BOOLEAN   [default: True]
  --gc-disable BOOLEAN   [default: True]
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""

EXTERNAL_HELP = """
Usage: conbench example-external [OPTIONS]

  Run example-external benchmark.

Options:
  --cpu-count INTEGER
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""

R_ONLY_HELP = """
Usage: conbench example-R-only [OPTIONS]

  Run example-R-only benchmark.

Options:
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: False]
  --cpu-count INTEGER
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


CASES_HELP = """
Usage: conbench example-cases [OPTIONS]

  Run example-cases benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --rows=10 --columns=10
  --rows=2 --columns=10
  --rows=10 --columns=2

  To run all combinations:
  $ conbench example-cases --all=true

Options:
  --rows [10|2]
  --columns [10|2]
  --all BOOLEAN          [default: False]
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: False]
  --gc-collect BOOLEAN   [default: True]
  --gc-disable BOOLEAN   [default: True]
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


cases_benchmark = _example_benchmarks.CasesBenchmark()
cases_exception = _example_benchmarks.CasesBenchmarkException()


def assert_simple_benchmark(result):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-simple",
        "cpu_count": None,
    }
    assert_context(munged)


def assert_simple_benchmark_exception(result):
    munged = copy.deepcopy(result)
    assert_context(munged)
    del munged["timestamp"]
    del munged["context"]
    assert munged == {
        "error": "division by zero",
        "tags": {"name": "example-simple-exception", "cpu_count": None},
    }


def assert_r_only_benchmark(result):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-R-only",
        "language": "R",
        "cpu_count": None,
    }
    assert_context(munged, language="R")


def assert_r_only_benchmark_exception(result):
    munged = copy.deepcopy(result)
    assert_context(munged, language="R")
    assert munged["tags"] == {
        "name": "example-R-only-exception",
        "cpu_count": None,
        "language": "R",
    }
    command = "run_one(arrowbench:::foo)"
    assert munged["command"] == f"library(arrowbench); {command}"
    assert "object 'foo' not found" in munged["error"]


def assert_r_only_benchmark_exception_no_result(result):
    munged = copy.deepcopy(result)
    assert_context(munged, language="R")
    assert munged["tags"] == {
        "name": "example-R-only-no-result",
        "cpu_count": None,
        "language": "R",
    }
    command = "run_one(arrowbench:::placebo, error_type=1)"
    assert munged["command"] == f"library(arrowbench); {command}"
    assert "Error in placebo_func" in munged["error"]


def assert_external_benchmark(result):
    munged = copy.deepcopy(result)
    assert_context(munged, language="C++")

    # assert tags
    assert munged["tags"] == {
        "name": "example-external",
        "cpu_count": None,
    }

    # assert stats
    del munged["stats"]["run_id"]
    del munged["stats"]["batch_id"]
    del munged["stats"]["timestamp"]
    assert munged["stats"] == {
        "data": ["100.000000", "200.000000", "300.000000"],
        "unit": "i/s",
        "times": ["0.100000", "0.200000", "0.300000"],
        "time_unit": "s",
        "iterations": 3,
        "mean": "200.000000",
        "median": "200.000000",
        "min": "100.000000",
        "max": "300.000000",
        "stdev": "100.000000",
        "q1": "150.000000",
        "q3": "250.000000",
        "iqr": "100.000000",
    }


def assert_cases_benchmark(result, case):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-cases",
        "cpu_count": None,
        "rows": case[0],
        "columns": case[1],
    }
    assert_context(munged)


def assert_cases_benchmark_exception(result, case):
    munged = copy.deepcopy(result)
    assert_context(munged)
    del munged["context"]
    del munged["timestamp"]
    assert munged == {
        "error": "division by zero",
        "tags": {
            "name": "example-cases-exception",
            "cpu_count": None,
            "rows": case[0],
            "columns": case[1],
        },
    }


def test_simple():
    benchmark = _example_benchmarks.SimpleBenchmark()
    [(result, output)] = benchmark.run(iterations=1)
    assert_simple_benchmark(result)
    assert output == 2


def test_simple_exception():
    benchmark = _example_benchmarks.SimpleBenchmarkException()
    [(result, output)] = benchmark.run(iterations=1)
    assert_simple_benchmark_exception(result)
    assert output is None


def test_simple_cli():
    command = ["conbench", "example-simple", "--help"]
    assert_cli(command, SIMPLE_HELP)


def test_external():
    benchmark = _example_benchmarks.ExternalBenchmark()
    [(result, output)] = benchmark.run()
    assert_external_benchmark(result)
    assert output == [100, 200, 300]


def test_external_cli():
    command = ["conbench", "example-external", "--help"]
    assert_cli(command, EXTERNAL_HELP)


def test_r_only():
    benchmark = _example_benchmarks.WithoutPythonBenchmark()
    [(result, output)] = benchmark.run()
    assert_r_only_benchmark(result)
    assert R_CLI in str(output)


def test_r_only_exception():
    benchmark = _example_benchmarks.BenchmarkExceptionR()
    [(result, output)] = benchmark.run()
    assert_r_only_benchmark_exception(result)
    assert output is None


def test_r_only_exception_no_result():
    benchmark = _example_benchmarks.BenchmarkExceptionNoResultR()
    [(result, output)] = benchmark.run()
    assert_r_only_benchmark_exception_no_result(result)
    assert output is None


def test_r_only_cli():
    command = ["conbench", "example-R-only", "--help"]
    assert_cli(command, R_ONLY_HELP)


@pytest.mark.parametrize("case", cases_benchmark.cases, ids=cases_benchmark.case_ids)
def test_cases(case):
    [(result, output)] = cases_benchmark.run(case, iterations=1)
    assert_cases_benchmark(result, case)
    assert isinstance(output, list)
    assert len(output) == int(case[0])  # rows


@pytest.mark.parametrize("case", cases_exception.cases, ids=cases_exception.case_ids)
def test_cases_exception(case):
    [(result, output)] = cases_exception.run(case)
    assert_cases_benchmark_exception(result, case)
    assert output is None


def test_cases_cli():
    command = ["conbench", "example-cases", "--help"]
    assert_cli(command, CASES_HELP)
