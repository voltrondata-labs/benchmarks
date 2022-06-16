import copy

import pytest

from .. import java_micro_benchmarks
from ..tests import _asserts

HELP = """
Usage: conbench java-micro [OPTIONS]

  Run the Arrow Java micro benchmarks.

Options:
  --src TEXT               Specify Arrow source directory.
  --benchmark-filter TEXT  Regex filtering benchmarks.
  --java-home TEXT         Path to Java Developers Kit.
  --java-options TEXT      Java compiler options.
  --iterations INTEGER     Number of iterations of each benchmark.
  --commit TEXT            Arrow commit.
  --show-result BOOLEAN    [default: true]
  --show-output BOOLEAN    [default: false]
  --run-id TEXT            Group executions together with a run id.
  --run-name TEXT          Free-text name of run (commit ABC, pull request
                           123, etc).
  --run-reason TEXT        Low-cardinality reason for run (commit, pull
                           request, manual, etc).
  --help                   Show this message and exit.
"""


def assert_benchmark(result):
    munged = copy.deepcopy(result)
    _asserts.assert_info_and_context(munged, language="Java")
    assert munged["tags"] == {
        "name": "setZero",
        "source": "java-micro",
        "suite": "arrow.memory.ArrowBufBenchmarks",
    }
    assert munged["stats"]["unit"] == "i/s"
    assert munged["stats"]["time_unit"] == "s"
    assert len(munged["stats"]["data"]) == 1
    assert len(munged["stats"]["times"]) == 0


def test_parse_benchmark_name():
    name = "org.apache.arrow.memory.ArrowBufBenchmarks.setZero"
    suite, name = java_micro_benchmarks._parse_benchmark_name(name)
    assert name == "setZero"
    assert suite == "arrow.memory.ArrowBufBenchmarks"


def test_get_run_command():
    options = {
        "iterations": 100,
        "benchmark_filter": "org.apache.arrow.memory.ArrowBufBenchmarks.setZero",
    }
    actual = java_micro_benchmarks.get_run_command("out", options)
    assert actual == [
        "archery",
        "benchmark",
        "run",
        "HEAD",
        "--language",
        "java",
        "--output",
        "out",
        "--repetitions",
        "100",
        "--benchmark-filter",
        "org.apache.arrow.memory.ArrowBufBenchmarks.setZero",
    ]


@pytest.mark.slow
def test_java_micro():
    benchmark = java_micro_benchmarks.RecordJavaMicroBenchmarks()
    benchmark_filter = "org.apache.arrow.memory.ArrowBufBenchmarks.setZero"
    [(result, output)] = benchmark.run(
        benchmark_filter=benchmark_filter,
        iterations=1,
    )
    assert_benchmark(result)
    assert benchmark_filter in str(output)


def test_java_micro_cli():
    command = ["conbench", "java-micro", "--help"]
    _asserts.assert_cli(command, HELP)
