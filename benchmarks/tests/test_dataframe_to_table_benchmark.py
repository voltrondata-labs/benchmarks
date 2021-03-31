import json

import pytest

from .. import _sources
from .. import dataframe_to_table_benchmark
from ..tests._asserts import assert_benchmark, assert_cli, R_CLI


HELP = """
Usage: conbench dataframe-to-table [OPTIONS] SOURCE

  Run dataframe-to-table benchmark.

Options:
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER   [default: 1]
  --gc-collect BOOLEAN   [default: true]
  --gc-disable BOOLEAN   [default: true]
  --show-result BOOLEAN  [default: true]
  --show-output BOOLEAN  [default: false]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
"""


chi_traffic = _sources.Source("chi_traffic_sample")

# TODO: ability to test R benchmarks with sample sources
type_strings_big = _sources.Source("type_strings")
type_dict_big = _sources.Source("type_dict")
type_integers_big = _sources.Source("type_integers")
type_floats_big = _sources.Source("type_floats")
type_nested_big = _sources.Source("type_nested")
simple_features_big = _sources.Source("type_simple_features")


def assert_run(run, index, benchmark, source):
    result, output = run[index]
    assert_benchmark(result, source.name, benchmark.name)
    assert "pyarrow.Table" in str(output)


def assert_run_r(run, index, benchmark, source):
    result, output = run[index]
    assert_benchmark(result, source.name, benchmark.name, language="R")
    assert R_CLI in str(output)


def test_dataframe_to_table_one():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(chi_traffic, iterations=1)
    assert_benchmark(result, chi_traffic.name, benchmark.name)
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_all():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    run = list(benchmark.run("TEST", iterations=1))
    assert len(run) == 7
    assert_run(run, 0, benchmark, chi_traffic)
    assert_run(run, 1, benchmark, type_strings_big)
    assert_run(run, 2, benchmark, type_dict_big)
    assert_run(run, 3, benchmark, type_integers_big)
    assert_run(run, 4, benchmark, type_floats_big)
    assert_run(run, 5, benchmark, type_nested_big)
    assert_run(run, 6, benchmark, simple_features_big)


def test_dataframe_to_table_one_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(chi_traffic, language="R")
    assert_benchmark(result, chi_traffic.name, benchmark.name, language="R")
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_all_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    run = list(benchmark.run("TEST", language="R"))
    assert len(run) == 7
    assert_run_r(run, 0, benchmark, chi_traffic)
    assert_run_r(run, 1, benchmark, type_strings_big)
    assert_run_r(run, 2, benchmark, type_dict_big)
    assert_run_r(run, 3, benchmark, type_integers_big)
    assert_run_r(run, 4, benchmark, type_floats_big)
    assert_run_r(run, 5, benchmark, type_nested_big)
    assert_run_r(run, 6, benchmark, simple_features_big)


def test_dataframe_to_table_cli():
    command = ["conbench", "dataframe-to-table", "--help"]
    assert_cli(command, HELP)
