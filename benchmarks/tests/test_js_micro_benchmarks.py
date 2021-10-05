import copy

import pytest

from .. import js_micro_benchmarks
from ..tests import _asserts

HELP = """
Usage: conbench js-micro [OPTIONS]

  Run Arrow JavaScript micro benchmarks.

Options:
  --src TEXT             Specify Arrow source directory.
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


def assert_benchmark(result):
    munged = copy.deepcopy(result)
    _asserts.assert_context(munged, language="JavaScript")
    assert munged["tags"] == {}  # TODO
    assert munged["stats"]["unit"] == "s"
    assert munged["stats"]["time_unit"] == "s"
    assert len(munged["stats"]["data"]) == 1
    assert len(munged["stats"]["times"]) == 0


def test_parse_benchmark_tags():
    name = "dataset: tracks, column: lng, length: 1,000,000, type: Float32"
    tags = js_micro_benchmarks._parse_benchmark_tags(name)
    assert tags == {
        "dataset": "tracks",
        "column": "lng",
        "length": "1,000,000",
        "type": "Float32",
    }

    name = "dataset: tracks, column: origin, length: 1,000,000, type: Dictionary<Int8, Utf8>, test: eq, value: Seattle"
    tags = js_micro_benchmarks._parse_benchmark_tags(name)
    assert tags == {
        "dataset": "tracks",
        "column": "origin",
        "length": "1,000,000",
        "test": "eq",
        "type": "Dictionary<Int8, Utf8>",
        "value": "Seattle",
    }

    # last value has a comma in it
    name = "dataset: tracks, column: origin, length: 1,000,000, type: Dictionary<Int8, Utf8>"
    tags = js_micro_benchmarks._parse_benchmark_tags(name)
    assert tags == {
        "dataset": "tracks",
        "column": "origin",
        "length": "1,000,000",
        "type": "Dictionary<Int8, Utf8>",
    }


def test_get_run_command():
    actual = js_micro_benchmarks.get_run_command()
    assert actual == ["yarn", "perf", "--json"]


@pytest.mark.slow
def test_javascript_micro():
    benchmark = js_micro_benchmarks.RecordJavaScriptMicroBenchmarks()
    [(result, output)] = benchmark.run()
    assert_benchmark(result)
    assert output is None


def test_javascript_micro_cli():
    command = ["conbench", "js-micro", "--help"]
    _asserts.assert_cli(command, HELP)
