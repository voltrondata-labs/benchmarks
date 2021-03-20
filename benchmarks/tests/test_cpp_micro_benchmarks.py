import copy
import json

import pytest

from .. import cpp_micro_benchmarks
from ..tests._asserts import assert_cli


HELP = """
Usage: conbench cpp-micro [OPTIONS]

  Run the Arrow C++ micro benchmarks.

Options:
  --src TEXT                 Specify Arrow source directory.
  --suite-filter TEXT        Regex filtering benchmark suites.
  --benchmark-filter TEXT    Regex filtering benchmarks.
  --cmake-extras TEXT        Extra flags/options to pass to cmake invocation.
  --cc TEXT                  C compiler.
  --cxx TEXT                 C++ compiler.
  --cxx-flags TEXT           C++ compiler flags.
  --cpp-package-prefix TEXT  Value to pass for ARROW_PACKAGE_PREFIX and use
                             ARROW_DEPENDENCY_SOURCE=SYSTEM.

  --iterations INTEGER       Number of iterations of each benchmark.
  --show-result BOOLEAN      [default: true]
  --show-output BOOLEAN      [default: false]
  --run-id TEXT              Group executions together with a run id.
  --help                     Show this message and exit.
"""


def test_parse_benchmark_name_no_params():
    tags = cpp_micro_benchmarks._parse_benchmark_name("Something")
    assert tags == {"name": "Something"}


def test_parse_benchmark_name_params():
    tags = cpp_micro_benchmarks._parse_benchmark_name("Something/1000")
    assert tags == {"name": "Something", "params": "1000"}


def test_parse_benchmark_name_many_params():
    tags = cpp_micro_benchmarks._parse_benchmark_name("Something/1000/33")
    assert tags == {"name": "Something", "params": "1000/33"}


def test_parse_benchmark_name_kind():
    name = "Something<Repetition::OPTIONAL, Compression::LZ4>"
    tags = cpp_micro_benchmarks._parse_benchmark_name(name)
    assert tags == {
        "name": "Something",
        "params": "<Repetition::OPTIONAL, Compression::LZ4>",
    }


def test_parse_benchmark_name_kind_and_params():
    name = "Something<Repetition::OPTIONAL, Compression::LZ4>/1000/33"
    tags = cpp_micro_benchmarks._parse_benchmark_name(name)
    assert tags == {
        "name": "Something",
        "params": "<Repetition::OPTIONAL, Compression::LZ4>/1000/33",
    }


def test_get_values():
    result = {
        "less_is_better": False,
        "name": "TakeStringRandomIndicesWithNulls/262144/1000",
        "time_unit": "ns",
        "times": [11391509.641413646],
        "unit": "items_per_second",
        "values": [23276243.284290202],
    }
    benchmark = cpp_micro_benchmarks.RecordCppMicroBenchmarks()
    actual = benchmark._get_values(result)
    assert actual == {
        "data": [23276243.284290202],
        "time_unit": "ns",
        "times": [11391509.641413646],
        "unit": "i/s",
    }


def test_format_unit():
    benchmark = cpp_micro_benchmarks.RecordCppMicroBenchmarks()
    assert benchmark._format_unit("bytes_per_second") == "B/s"
    assert benchmark._format_unit("items_per_second") == "i/s"
    assert benchmark._format_unit("foo_per_bar") == "foo_per_bar"


def test_get_run_command():
    options = {
        "iterations": 100,
        "suite_filter": "arrow-compute-vector-selection-benchmark",
        "benchmark_filter": "TakeStringRandomIndicesWithNulls/262144/2",
    }
    actual = cpp_micro_benchmarks.get_run_command("out", options)
    assert actual == [
        "archery",
        "benchmark",
        "run",
        "--output",
        "out",
        "--repetitions",
        "100",
        "--suite-filter",
        "arrow-compute-vector-selection-benchmark",
        "--benchmark-filter",
        "TakeStringRandomIndicesWithNulls/262144/2",
    ]


def assert_benchmark(result):
    munged = copy.deepcopy(result)
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
    assert "arrow_version" in munged["context"]
    assert munged["context"]["benchmark_language"] == "C++"


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
    print(json.dumps(result, indent=4, sort_keys=True))
    assert benchmark_filter in str(output)


def test_cpp_micro_cli():
    command = ["conbench", "cpp-micro", "--help"]
    assert_cli(command, HELP)
