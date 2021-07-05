import copy

import pytest

from .. import js_micro_benchmarks
from ..tests._asserts import assert_cli, assert_context


HELP = """
Usage: conbench js-micro [OPTIONS]

  Run the Arrow JavaScript micro benchmarks.

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
    assert_context(munged, language="JavaScript")
    assert munged["tags"] == {}  # TODO
    assert munged["stats"]["unit"] == "s"
    assert munged["stats"]["time_unit"] == "s"
    assert len(munged["stats"]["data"]) == 1
    assert len(munged["stats"]["times"]) == 0


def test_parse_benchmark_name_kind():
    name = (
        "name: 'origin', length: 1,000,000, type: Dictionary<Int8, Utf8>, test: eq, value: Seattle",
    )
    parsed = js_micro_benchmarks._parse_benchmark_name(name)
    assert parsed == name


def test_get_run_command():
    actual = js_micro_benchmarks.get_run_command("out.json")
    assert actual == [
        "yarn",
        "perf",
        "--json",
        "out.json",
    ]


@pytest.mark.slow
def test_javascript_micro():
    benchmark = js_micro_benchmarks.RecordJavaScriptMicroBenchmarks()
    [(result, output)] = benchmark.run()
    assert_benchmark(result)
    assert output is None


def test_javascript_micro_cli():
    command = ["conbench", "js-micro", "--help"]
    assert_cli(command, HELP)
