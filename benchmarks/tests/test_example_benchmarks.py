import copy

import pytest

from .. import _example_benchmarks
from ..tests import _asserts

SIMPLE_HELP = """
Usage: conbench example-simple [OPTIONS]

  Run example-simple benchmark.

Options:
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: false]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Free-text name of run (commit ABC, pull request 123,
                         etc).
  --run-reason TEXT      Low-cardinality reason for run (commit, pull request,
                         manual, etc).
  --help                 Show this message and exit.
"""

EXTERNAL_HELP = """
Usage: conbench example-external [OPTIONS]

  Run example-external benchmark.

Options:
  --cpu-count INTEGER
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Free-text name of run (commit ABC, pull request 123,
                         etc).
  --run-reason TEXT      Low-cardinality reason for run (commit, pull request,
                         manual, etc).
  --help                 Show this message and exit.
"""

R_ONLY_HELP = """
Usage: conbench example-R-only [OPTIONS]

  Run example-R-only benchmark.

Options:
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: false]
  --cpu-count INTEGER
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Free-text name of run (commit ABC, pull request 123,
                         etc).
  --run-reason TEXT      Low-cardinality reason for run (commit, pull request,
                         manual, etc).
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
  --all BOOLEAN          [default: false]
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: false]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Free-text name of run (commit ABC, pull request 123,
                         etc).
  --run-reason TEXT      Low-cardinality reason for run (commit, pull request,
                         manual, etc).
  --help                 Show this message and exit.
"""


cases_benchmark = _example_benchmarks.CasesBenchmark()
cases_exception = _example_benchmarks.CasesBenchmarkException()


def test_simple():
    benchmark = _example_benchmarks.SimpleBenchmark()
    [(result, output)] = benchmark.run(iterations=1)

    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-simple",
        "cpu_count": None,
    }
    _asserts.assert_info_and_context(munged)
    assert output == 2


def test_simple_exception():
    benchmark = _example_benchmarks.SimpleBenchmarkException()
    [(result, output)] = benchmark.run(iterations=1)

    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged)
    assert "timestamp" in munged
    assert munged["error"]["error"] == "division by zero"
    expected_tags = {"name": "example-simple-exception", "cpu_count": None}
    assert munged["tags"] == expected_tags
    assert output is None


def test_simple_cli():
    command = ["conbench", "example-simple", "--help"]
    _asserts.assert_cli(command, SIMPLE_HELP)


def test_external():
    benchmark = _example_benchmarks.ExternalBenchmark()
    [(result, output)] = benchmark.run()

    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged, language="C++")

    # assert tags
    assert munged["tags"] == {
        "name": "example-external",
        "cpu_count": None,
    }

    # assert stats
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
    assert output == [100, 200, 300]


def test_external_cli():
    command = ["conbench", "example-external", "--help"]
    _asserts.assert_cli(command, EXTERNAL_HELP)


def test_r_only():
    benchmark = _example_benchmarks.WithoutPythonBenchmark()
    [(result, output)] = benchmark.run()

    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-R-only",
        "language": "R",
        "cpu_count": None,
    }
    _asserts.assert_info_and_context(munged, language="R")
    assert _asserts.R_CLI in str(output)


def test_r_only_nonexistent():
    benchmark = _example_benchmarks.BenchmarkNonexistentR()
    [(result, output)] = benchmark.run()

    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged, language="R")
    assert munged["tags"] == {
        "name": "example-R-only-nonexistent",
        "cpu_count": None,
        "language": "R",
    }
    command = "run_one(arrowbench:::foo)"
    assert benchmark._get_r_command() == f"library(arrowbench); {command}"
    assert "object 'foo' not found" in munged["error"]["error"]
    assert output is None


@pytest.mark.parametrize("error_type", ["NULL"])
@pytest.mark.parametrize("output_type", ["NULL", "'warning'"])
def test_r_only_placebo(error_type, output_type):
    benchmark = _example_benchmarks.BenchmarkPlaceboR()
    [(result, output)] = benchmark.run(case=(error_type, output_type))

    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged, language="R")
    assert munged["tags"] == {
        "name": "example-R-only-exception",
        "error_type": error_type,
        "output_type": output_type,
        "cpu_count": None,
        "language": "R",
    }
    command = (
        "run_one(arrowbench:::placebo, "
        f"error_type={error_type}, "
        f"output_type={output_type})"
    )
    assert (
        benchmark._get_r_command((error_type, output_type))
        == f"library(arrowbench); {command}"
    )
    assert "error" not in munged
    assert float(munged["stats"]["data"][0]) > 0.0
    if output_type == "'warning'":
        assert "A warning" in munged["optional_benchmark_info"]["output"]
    else:
        assert "A warning" not in munged["optional_benchmark_info"]["output"]
    assert command in output


@pytest.mark.parametrize("error_type", ["'base'"])
@pytest.mark.parametrize("output_type", ["NULL", "'warning'"])
def test_r_only_exception(error_type: str, output_type: str):
    benchmark = _example_benchmarks.BenchmarkPlaceboR()
    [(result, output)] = benchmark.run(case=(error_type, output_type))

    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged, language="R")
    assert munged["tags"] == {
        "name": "example-R-only-exception",
        "error_type": error_type,
        "output_type": output_type,
        "cpu_count": None,
        "language": "R",
    }
    command = (
        "run_one(arrowbench:::placebo, "
        f"error_type={error_type}, "
        f"output_type={output_type})"
    )
    assert (
        benchmark._get_r_command((error_type, output_type))
        == f"library(arrowbench); {command}"
    )
    assert "something went wrong (but I knew that)" in munged["error"]["error"]
    if output_type == "'warning'":
        assert "warnings" in munged["error"]
    else:
        assert "warnings" not in munged["error"]
    assert command in output


def test_r_only_cli():
    command = ["conbench", "example-R-only", "--help"]
    _asserts.assert_cli(command, R_ONLY_HELP)


@pytest.mark.parametrize("case", cases_benchmark.cases, ids=cases_benchmark.case_ids)
def test_cases(case):
    [(result, output)] = cases_benchmark.run(case, iterations=1)

    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-cases",
        "cpu_count": None,
        "rows": case[0],
        "columns": case[1],
        "case_version": 2 if case[0] == "2" else 1,
    }
    _asserts.assert_info_and_context(munged)
    assert isinstance(output, list)
    assert len(output) == int(case[0])  # rows


@pytest.mark.parametrize("case", cases_exception.cases, ids=cases_exception.case_ids)
def test_cases_exception(case):
    [(result, output)] = cases_exception.run(case)

    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged)
    assert "timestamp" in munged
    assert munged["error"]["error"] == "division by zero"
    expected_tags = {
        "name": "example-cases-exception",
        "cpu_count": None,
        "rows": case[0],
        "columns": case[1],
        "case_version": 2 if case[0] == "2" else 1,
    }
    assert munged["tags"] == expected_tags
    assert output is None


def test_cases_cli():
    command = ["conbench", "example-cases", "--help"]
    _asserts.assert_cli(command, CASES_HELP)
