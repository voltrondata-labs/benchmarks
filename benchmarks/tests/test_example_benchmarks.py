import copy
import json

import pytest

from .. import _sources
from .. import _example_benchmarks
from ..tests._asserts import assert_context, assert_cli


SIMPLE_HELP = """
Usage: conbench example-simple [OPTIONS]

  Run example-simple benchmark.

Options:
  --iterations INTEGER   [default: 1]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --help                 Show this message and exit.
"""

EXTERNAL_HELP = """
Usage: conbench example-external [OPTIONS]

  Run example-external benchmark.

Options:
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
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
  --gc-collect BOOLEAN         [default: true]
  --gc-disable BOOLEAN         [default: true]
  --show-result BOOLEAN        [default: true]
  --show-output BOOLEAN        [default: false]
  --run-id TEXT                Group executions together with a run id.
  --help                       Show this message and exit.
"""


nyctaxi = _sources.Source("nyctaxi_sample")
cases_benchmark = _example_benchmarks.CasesBenchmark()


def assert_cases_benchmark(result, case, source):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-cases",
        "count": 1,
        "dataset": source,
        "gc_collect": True,
        "gc_disable": True,
        "color": case[0],
        "fruit": case[1],
    }
    assert_context(munged)


def assert_simple_benchmark(result):
    munged = copy.deepcopy(result)
    assert munged["tags"] == {
        "name": "example-simple",
        "year": "2020",
        "gc_collect": True,
        "gc_disable": True,
    }
    assert_context(munged)


def assert_external_benchmark(result):
    munged = copy.deepcopy(result)

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

    # assert context
    context = list(munged["context"].keys())
    for c in context:
        if c.startswith("arrow"):
            munged["context"].pop(c)
    assert munged["context"] == {"benchmark_language": "C++"}


def test_simple():
    benchmark = _example_benchmarks.SimpleBenchmark()
    [(result, output)] = benchmark.run(iterations=1)
    assert_simple_benchmark(result)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert output == "hello!"


def test_simple_cli():
    command = ["conbench", "example-simple", "--help"]
    assert_cli(command, SIMPLE_HELP)


def test_external():
    benchmark = _example_benchmarks.RecordExternalBenchmark()
    [(result, output)] = benchmark.run()
    assert_external_benchmark(result)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert output == [100, 200, 300]


def test_external_cli():
    command = ["conbench", "example-external", "--help"]
    assert_cli(command, EXTERNAL_HELP)


@pytest.mark.parametrize("case", cases_benchmark.cases, ids=cases_benchmark.case_ids)
def test_cases(case):
    [(result, output)] = cases_benchmark.run(nyctaxi, case, iterations=1)
    assert_cases_benchmark(result, case, nyctaxi.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "nyctaxi_sample" in output


def test_cases_cli():
    command = ["conbench", "example-cases", "--help"]
    assert_cli(command, CASES_HELP)
