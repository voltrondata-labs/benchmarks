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
  --help                 Show this message and exit.
"""


chi_traffic = _sources.Source("chi_traffic_sample")

# TODO: ability to test R benchmarks with sample sources
chi_traffic_big = _sources.Source("chi_traffic_2020_Q1")
type_strings_big = _sources.Source("sample_strings")
type_dict_big = _sources.Source("sample_dict")
type_integers_big = _sources.Source("sample_integers")
type_floats_big = _sources.Source("sample_floats")
type_nested_big = _sources.Source("sample_nested")
simple_features_big = _sources.Source("sample_simple_features")


def test_dataframe_to_table_chi_traffic():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(chi_traffic, iterations=1)
    assert_benchmark(result, chi_traffic.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_strings():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_strings_big, iterations=1)
    assert_benchmark(result, type_strings_big.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_dict():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_dict_big, iterations=1)
    assert_benchmark(result, type_dict_big.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_integers():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_integers_big, iterations=1)
    assert_benchmark(result, type_integers_big.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_floats():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_floats_big, iterations=1)
    assert_benchmark(result, type_floats_big.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_nested():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_nested_big, iterations=1)
    assert_benchmark(result, type_nested_big.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_simple_features():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(simple_features_big, iterations=1)
    assert_benchmark(result, simple_features_big.name, benchmark.name)
    print(json.dumps(result, indent=4, sort_keys=True))
    assert "pyarrow.Table" in str(output)


@pytest.mark.slow
def test_dataframe_to_table_chi_traffic_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(chi_traffic_big, language="R")
    assert_benchmark(result, chi_traffic_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_strings_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_strings_big, language="R")
    assert_benchmark(result, type_strings_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_dict_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_dict_big, language="R")
    assert_benchmark(result, type_dict_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_integers_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_integers_big, language="R")
    assert_benchmark(result, type_integers_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_floats_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_floats_big, language="R")
    assert_benchmark(result, type_floats_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_type_nested_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(type_nested_big, language="R")
    assert_benchmark(result, type_nested_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


@pytest.mark.slow
def test_dataframe_to_table_simple_features_r():
    benchmark = dataframe_to_table_benchmark.DataframeToTableBenchmark()
    [(result, output)] = benchmark.run(simple_features_big, language="R")
    assert_benchmark(result, simple_features_big.name, benchmark.name, language="R")
    print(json.dumps(result, indent=4, sort_keys=True))
    assert R_CLI in str(output)


def test_dataframe_to_table_cli():
    command = ["conbench", "dataframe-to-table", "--help"]
    assert_cli(command, HELP)
