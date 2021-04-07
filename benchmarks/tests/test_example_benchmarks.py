import copy
import json

import pytest

from .. import _sources
from .. import _example_benchmarks
from ..tests._asserts import assert_context, assert_cli, R_CLI


SIMPLE_HELP = """
Usage: conbench example-simple [OPTIONS]

  Run example-simple benchmark.

Options:
  --iterations INTEGER   [default: 1]
  --drop-caches BOOLEAN  [default: false]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""

EXTERNAL_HELP = """
Usage: conbench example-external [OPTIONS]

  Run example-external benchmark.

Options:
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""

R_ONLY_HELP = """
Usage: conbench example-R-only [OPTIONS]

  Run example-R-only benchmark.

Options:
  --iterations INTEGER   [default: 1]
  --cpu-count INTEGER
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


CASES_HELP = """
Usage: conbench example-cases [OPTIONS] SOURCE

  Run example-cases benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --color=pink --fruit=apple
  --color=yellow --fruit=apple
  --color=green --fruit=apple
  --color=yellow --fruit=orange
  --color=pink --fruit=orange

  To run all combinations:
  $ conbench example-cases --all=true

Options:
  --color [green|pink|yellow]
  --fruit [apple|orange]
  --all BOOLEAN                [default: false]
  --count INTEGER              [default: 1]
  --iterations INTEGER         [default: 1]
  --drop-caches BOOLEAN        [default: false]
  --gc-collect BOOLEAN         [default: true]
  --gc-disable BOOLEAN         [default: true]
  --show-result BOOLEAN        [default: true]
  --show-output BOOLEAN        [default: false]
  --run-id TEXT                Group executions together with a run id.
  --run-name TEXT              Name of run (commit, pull request, etc).
  --help                       Show this message and exit.
"""


nyctaxi = _sources.Source("nyctaxi_sample")
cases_benchmark = _example_benchmarks.CasesBenchmark()
cases_exception = _example_benchmarks.CasesBenchmarkException()


def assert_simple_benchmark(result):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-simple",
        "year": "2020",
    }
    assert_context(munged)


def assert_simple_benchmark_exception(result):
    munged = copy.deepcopy(result)
    assert_context(munged)
    del munged["timestamp"]
    del munged["context"]
    assert munged == {
        "error": "division by zero",
        "tags": {"name": "example-simple-exception", "year": "2020"},
    }


def assert_r_only_benchmark(result):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-R-only",
        "year": "2020",
        "language": "R",
        "cpu_count": None,
    }
    assert_context(munged, language="R")


def assert_r_only_benchmark_exception(result):
    munged = copy.deepcopy(result)
    assert_context(munged, language="R")
    del munged["context"]
    del munged["timestamp"]
    assert munged == {
        "tags": {
            "name": "example-R-only-exception",
            "year": "2020",
            "language": "R",
        },
        "command": "library(arrowbench); run_one(arrowbench:::foo)",
        "error": "Error in get(name, envir = asNamespace(pkg), inherits = FALSE) : \n"
        "  object 'foo' not found\n"
        "Calls: run_one -> modifyList -> stopifnot -> ::: -> get\n"
        "Execution halted",
    }


def assert_r_only_benchmark_exception_no_result(result):
    munged = copy.deepcopy(result)
    assert_context(munged, language="R")
    del munged["context"]
    del munged["timestamp"]
    assert munged == {
        "tags": {
            "name": "example-R-only-no-result",
            "year": "2020",
            "language": "R",
        },
        "command": "library(arrowbench); run_one(arrowbench:::placebo, error_type=1)",
        "error": "Error: Error in placebo_func() : something went wrong (but I knew that)\n"
        "Calls: run_bm ... eval.parent -> eval -> eval -> eval -> placebo_func\n"
        "Execution halted",
    }


def assert_external_benchmark(result):
    munged = copy.deepcopy(result)
    assert_context(munged, language="C++")

    # assert tags
    assert munged["tags"] == {
        "name": "example-external",
        "year": "2020",
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


def assert_cases_benchmark(result, case, source):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-cases",
        "count": 1,
        "dataset": source,
        "color": case[0],
        "fruit": case[1],
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
            "year": "2020",
            "color": case[0],
            "fruit": case[1],
        },
    }


def test_simple():
    benchmark = _example_benchmarks.SimpleBenchmark()
    [(result, output)] = benchmark.run(iterations=1)
    assert_simple_benchmark(result)
    assert output == "hello!"


def test_simple_exception():
    benchmark = _example_benchmarks.SimpleBenchmarkException()
    [(result, output)] = benchmark.run(iterations=1)
    assert_simple_benchmark_exception(result)
    assert output is None


def test_simple_cli():
    command = ["conbench", "example-simple", "--help"]
    assert_cli(command, SIMPLE_HELP)


def test_external():
    benchmark = _example_benchmarks.RecordExternalBenchmark()
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
    [(result, output)] = cases_benchmark.run(nyctaxi, case, iterations=1)
    assert_cases_benchmark(result, case, nyctaxi.name)
    assert "nyctaxi_sample" in output


@pytest.mark.parametrize("case", cases_exception.cases, ids=cases_exception.case_ids)
def test_cases_exception(case):
    [(result, output)] = cases_exception.run(case)
    assert_cases_benchmark_exception(result, case)
    assert output is None


def test_cases_cli():
    command = ["conbench", "example-cases", "--help"]
    assert_cli(command, CASES_HELP)
