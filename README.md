<p align="right">
<a href="https://github.com/ursacomputing/benchmarks/blob/main/.github/workflows/actions.yml"><img alt="Build Status" src="https://github.com/ursacomputing/benchmarks/actions/workflows/actions.yml/badge.svg?branch=main"></a>
<a href="https://coveralls.io/github/ursacomputing/benchmarks?branch=main"><img src="https://coveralls.io/repos/github/ursacomputing/benchmarks/badge.svg?branch=main&kill_cache=b6152efa2cc8d108c18e6f8688fa01aeba8b5b3b" alt="Coverage Status" /></a>
<a href="https://github.com/psf/black"><img alt="Code style: black" src="https://img.shields.io/badge/code%20style-black-000000.svg"></a>
</p>


# Apache Arrow Benchmarks

<b>Language-independent Continuous Benchmarking (CB) for Apache Arrow</b>
<br/>


This package contains Python macro benchmarks for Apache Arrow, as well
as benchmarks that execute and record the results for both the Arrow
C++ micro benchmarks and the Arrow R macro benchmarks (which are found
in the [arrowbench](https://github.com/ursacomputing/arrowbench)
repository). These benchmarks use the
[Conbench runner](https://github.com/ursacomputing/conbench) for
benchmark execution, and the results are published to Arrow's public
[Conbench server](https://conbench.ursa.dev/).

On each commit to the main [Arrow](https://github.com/apache/arrow)
branch, the C++, Python, and R benchmarks are run on a variety of
physical benchmarking machines & EC2 instances of different sizes, and
the results are published to Conbench. Additionally, benchmarks can
also be run on an Arrow pull request by adding a GitHub comment with
the text: **`@ursabot please benchmark`**. A baseline benchmarking run
against the pull request's head with also be scheduled, and Conbench
comparison links will be posted as a follow-up GitHub comment.

You can also filter the pull request benchmarks runs by filter name,
language, or specific command. A GitHub comment with text
**`@ursabot benchmark help`** will follow-up with a list of available
ursabot benchmark commands.

```
@ursabot benchmark help
@ursabot please benchmark
@ursabot please benchmark lang=Python
@ursabot please benchmark lang=C++
@ursabot please benchmark lang=R
@ursabot please benchmark name=file-write
@ursabot please benchmark name=file-write lang=Python
@ursabot please benchmark name=file-.*
@ursabot please benchmark command=cpp-micro --suite-filter=arrow-compute-vector-selection-benchmark --benchmark-filter=TakeStringRandomIndicesWithNulls/262144/2 --iterations=3
```

Benchmarks added to this repository and declared in
[benchmarks.json](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks.json)
will automatically be picked up by by Arrow's Continuous Benchmarking
pipeline. This file is regenerated each time the unit tests are run
based on the various benchmark class attributes. See the
[`BenchmarkList`](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/_benchmark.py)
class for more information on how to override any of the benchmark
defaults or to disable a particular benchmark.


## Index

* [Contributing](https://github.com/ursacomputing/benchmarks#contributing)
* [Running benchmarks](https://github.com/ursacomputing/benchmarks#running-benchmarks)
* [Authoring benchmarks](https://github.com/ursacomputing/benchmarks#authoring-benchmarks)


## Contributing


### Create workspace
    $ cd
    $ mkdir -p envs
    $ mkdir -p workspace
    $ mkdir -p data
    $ export BENCHMARKS_DATA_DIR=$(pwd)/data
    $ export ARROWBENCH_DATA_DIR=$(pwd)/data


### Create virualenv
    $ cd ~/envs
    $ python3 -m venv qa
    $ source qa/bin/activate


### Clone repos
    (qa) $ cd ~/workspace/
    (qa) $ git clone https://github.com/ursacomputing/benchmarks.git
    (qa) $ git clone https://github.com/ursacomputing/conbench.git
    (qa) $ git clone https://github.com/apache/arrow.git


### Install benchmarks dependencies
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ pip install -r requirements-test.txt
    (qa) $ pip install -r requirements-build.txt
    (qa) $ python setup.py develop


### Install arrowbench (to run R benchmarks)
    $ R
    > install.packages('remotes')
    > remotes::install_github("ursacomputing/arrowbench")


### Install archery (to run C++ micro benchmarks)
    (qa) $ cd ~/workspace/
    (qa) $ pip install -e arrow/dev/archery


### Install conbench dependencies
    (qa) $ cd ~/workspace/conbench/
    (qa) $ pip install -r requirements-test.txt
    (qa) $ pip install -r requirements-build.txt
    (qa) $ pip install -r requirements-cli.txt
    (qa) $ python setup.py install


### Conbench credentials default to this following (edit .conbench to configure)

(This is only needed if you plan on publishing benchmark results to a Conbench server.)

    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ cat .conbench
    url: http://localhost:5000
    email: conbench@example.com
    password: conbench


### Run tests
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ pytest -vv benchmarks/tests/


### Format code (before committing)
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ git status
        modified: foo.py
    (qa) $ black foo.py
        reformatted foo.py
    (qa) $ git add foo.py


### Lint code (before committing)
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ flake8
    ./foo/bar/__init__.py:1:1: F401 'FooBar' imported but unused


### Generate coverage report
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ coverage run --source benchmarks -m pytest benchmarks/tests/
    (qa) $ coverage report -m


## Running benchmarks


### Run benchmarks as tests
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ pytest -vv --capture=no benchmarks/tests/test_file_benchmark.py
    test_file_benchmark.py::test_read_taxi[feather, zstd, table] PASSED
    test_file_benchmark.py::test_read_taxi[feather, zstd, dataframe] PASSED
    test_file_benchmark.py::test_write_fanniemae[parquet, uncompressed, table] PASSED
    test_file_benchmark.py::test_write_fanniemae[parquet, uncompressed, dataframe] PASSED
    ...


### Run benchmarks from command line

Conbench can be run from either of the following directories.

    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ cd ~/workspace/benchmarks/benchmarks/


Use the `conbench --help` command to see the available benchmarks.

```
(qa) $ conbench --help
Usage: conbench [OPTIONS] COMMAND [ARGS]...

  Conbench: Language-independent Continuous Benchmarking (CB) Framework

Options:
  --help  Show this message and exit.

Commands:
  cpp-micro                   Run the Arrow C++ micro benchmarks.
  csv-read                    Run csv-read benchmark.
  dataframe-to-table          Run dataframe-to-table benchmark.
  dataset-filter              Run dataset-filter benchmark.
  dataset-read                Run dataset-read benchmark(s).
  dataset-select              Run dataset-select benchmark.
  dataset-selectivity         Run dataset-selectivity benchmark(s).
  example-R-only              Run example-R-only benchmark.
  example-R-only-exception    Run example-R-only-exception benchmark.
  example-R-only-no-result    Run example-R-only-no-result benchmark.
  example-cases               Run example-cases benchmark(s).
  example-cases-exception     Run example-cases-exception benchmark(s).
  example-external            Run example-external benchmark.
  example-simple              Run example-simple benchmark.
  example-simple-exception    Run example-simple-exception benchmark.
  file-read                   Run file-read benchmark(s).
  file-write                  Run file-write benchmark(s).
  list                        List of benchmarks (for orchestration).
  partitioned-dataset-filter  Run partitioned-dataset-filter benchmark(s).
  wide-dataframe              Run wide-dataframe benchmark(s).
```

Help is also available for individual benchmark commands.

```
(qa) $ conbench file-write --help
Usage: conbench file-write [OPTIONS] SOURCE

  Run file-write benchmark(s).

  For each benchmark option, the first option value is the default.

  Valid benchmark combinations:
  --file-type=parquet --compression=uncompressed --input-type=table
  --file-type=parquet --compression=uncompressed --input-type=dataframe
  --file-type=parquet --compression=lz4 --input-type=table
  --file-type=parquet --compression=lz4 --input-type=dataframe
  --file-type=parquet --compression=zstd --input-type=table
  --file-type=parquet --compression=zstd --input-type=dataframe
  --file-type=parquet --compression=snappy --input-type=table
  --file-type=parquet --compression=snappy --input-type=dataframe
  --file-type=feather --compression=uncompressed --input-type=table
  --file-type=feather --compression=uncompressed --input-type=dataframe
  --file-type=feather --compression=lz4 --input-type=table
  --file-type=feather --compression=lz4 --input-type=dataframe
  --file-type=feather --compression=zstd --input-type=table
  --file-type=feather --compression=zstd --input-type=dataframe

  To run all combinations:
  $ conbench file-write --all=true

Options:
  --file-type [feather|parquet]
  --compression [lz4|snappy|uncompressed|zstd]
  --input-type [dataframe|table]
  --all BOOLEAN                   [default: False]
  --language [Python|R]
  --cpu-count INTEGER
  --iterations INTEGER            [default: 1]
  --drop-caches BOOLEAN           [default: False]
  --gc-collect BOOLEAN            [default: True]
  --gc-disable BOOLEAN            [default: True]
  --show-result BOOLEAN           [default: True]
  --show-output BOOLEAN           [default: False]
  --run-id TEXT                   Group executions together with a run id.
  --run-name TEXT                 Name of run (commit, pull request, etc).
  --help                          Show this message and exit.
```

Example benchmark execution.

```
(qa) $ conbench file-read nyctaxi_sample --file-type=parquet --iterations=10 --gc-disable=false

Benchmark result:
{
    "context": {
        "arrow_compiler_flags": "-fPIC -arch x86_64 -arch x86_64 -std=c++11 -Qunused-arguments -fcolor-diagnostics -O3 -DNDEBUG",
        "arrow_compiler_id": "AppleClang",
        "arrow_compiler_version": "12.0.0.12000032",
        "arrow_version": "4.0.0",
        "benchmark_language": "Python",
        "benchmark_language_version": "Python 3.9.2"
    },
    "github": {
        "commit": "f959141ece4d660bce5f7fa545befc0116a7db79",
        "repository": "https://github.com/apache/arrow"
    },
    "machine_info": {
        "architecture_name": "x86_64",
        "cpu_core_count": "2",
        "cpu_frequency_max_hz": "3500000000",
        "cpu_l1d_cache_bytes": "32768",
        "cpu_l1i_cache_bytes": "32768",
        "cpu_l2_cache_bytes": "262144",
        "cpu_l3_cache_bytes": "4194304",
        "cpu_model_name": "Intel(R) Core(TM) i7-7567U CPU @ 3.50GHz",
        "cpu_thread_count": "4",
        "kernel_name": "20.5.0",
        "memory_bytes": "17179869184",
        "name": "machine-abc",
        "os_name": "macOS",
        "os_version": "10.16"
    },
    "stats": {
        "batch_id": "ccb3a04d20ce49ed8be620671129c3ce",
        "data": [
            "0.011945",
            "0.005703",
            "0.003593",
            "0.003556",
            "0.003648",
            "0.003815",
            "0.003887",
            "0.003463",
            "0.003822",
            "0.003223"
        ],
        "iqr": "0.000305",
        "iterations": 10,
        "max": "0.011945",
        "mean": "0.004665",
        "median": "0.003731",
        "min": "0.003223",
        "q1": "0.003565",
        "q3": "0.003871",
        "run_id": "ccb3a04d20ce49ed8be620671129c3ce",
        "stdev": "0.002647",
        "time_unit": "s",
        "times": [],
        "timestamp": "2021-06-22T20:48:09.761129+00:00",
        "unit": "s"
    },
    "tags": {
        "compression": "lz4",
        "cpu_count": null,
        "dataset": "nyctaxi_sample",
        "file_type": "parquet",
        "name": "file-read",
        "output_type": "dataframe"
    }
}
```


## Authoring benchmarks

There are three main types of benchmarks: "simple benchmarks" that time
the execution of a unit of work, "external benchmarks" that just record
benchmark results that were obtained from some other benchmarking tool,
and "case benchmarks" which benchmark a unit of work under different scenarios.

Included in this repository are contrived, minimal examples of these
different kinds of benchmarks to be used as templates for benchmark
authoring. These example benchmarks and their tests can be found here:

* [_example_benchmarks.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/_example_benchmarks.py)
* [test_example_benchmarks.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/tests/test_example_benchmarks.py)


### Example simple benchmarks

A "simple benchmark" runs and records the execution time of a unit of work.

Implementation details: Note that this benchmark extends
`benchmarks._benchmark.Benchmark`, implements the minimum required `run()`
method, and registers itself with the `@conbench.runner.register_benchmark`
decorator.

```python
@conbench.runner.register_benchmark
class SimpleBenchmark(_benchmark.Benchmark):
    """Example benchmark without cases."""

    name = "example-simple"

    def run(self, **kwargs):
        tags = self.get_tags(kwargs)
        f = self._get_benchmark_function()
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self):
        return lambda: 1 + 1
```

```
$ conbench example-simple --help

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
```


More simple benchmark examples that have minimal scaffolding:

* [csv_read_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/csv_read_benchmark.py)
* [dataset_filter_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/dataset_filter_benchmark.py)


### Example external benchmarks

An "external benchmark" records results that were obtained from some other
benchmarking tool (like executing the Arrow C++ micro benchmarks from command
line, parsing the resulting JSON, and recording those results).

Implementation details: Note that the following benchmark sets
`external = True`, and calls `record()` rather than `benchmark()` as the
example above does.

```python
@conbench.runner.register_benchmark
class ExternalBenchmark(_benchmark.Benchmark):
    """Example benchmark that just records external results."""

    external = True
    name = "example-external"

    def run(self, **kwargs):
        # external results from somewhere
        # (an API call, command line execution, etc)
        result = {
            "data": [100, 200, 300],
            "unit": "i/s",
            "times": [0.100, 0.200, 0.300],
            "time_unit": "s",
        }

        tags = self.get_tags(kwargs)
        context = {"benchmark_language": "C++"}
        yield self.record(
            result,
            tags,
            context,
            kwargs,
            output=result["data"],
        )
```

```
$ conbench example-external --help

Usage: conbench example-external [OPTIONS]

  Run example-external benchmark.

Options:
  --cpu-count INTEGER
  --show-result BOOLEAN  [default: True]
  --show-output BOOLEAN  [default: False]
  --run-id TEXT          Group executions together with a run id.
  --run-name TEXT        Name of run (commit, pull request, etc).
  --help                 Show this message and exit.
```

Implementation details: Note that the following benchmark extends `BenchmarkR`,
sets both `external` and `r_only` to `True`, defines `r_name`, implements
`_get_r_command()`, and calls `r_benchmark()` rather than `benchmark()` or
`record()`.

```python
@conbench.runner.register_benchmark
class WithoutPythonBenchmark(_benchmark.BenchmarkR):
    """Example R benchmark that doesn't have a Python equivalent."""

    external, r_only = True, True
    name, r_name = "example-R-only", "placebo"

    def run(self, **kwargs):
        tags = self.get_tags(kwargs)
        command = self._get_r_command(kwargs)
        yield self.r_benchmark(command, tags, kwargs)

    def _get_r_command(self, options):
        return (
            f"library(arrowbench); "
            f"run_one(arrowbench:::{self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)})"
        )
```

```
$ conbench example-R-only --help

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
```

More external benchmark examples that record C++ and R benchmark results:

* [cpp_micro_benchmarks.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/cpp_micro_benchmarks.py)
* [dataframe_to_table_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/dataframe_to_table_benchmark.py)


### Example case benchmarks

A "case benchmark" is a either a "simple benchmark" or an "external benchmark"
executed under various predefined scenarios (cases).

Implementation details: Note that the following benchmark declares the valid
combinations in `valid_cases`, which reads like a CSV (the first row contains
the cases names).


```python
@conbench.runner.register_benchmark
class CasesBenchmark(_benchmark.Benchmark):
    """Example benchmark with cases."""

    name = "example-cases"
    valid_cases = (
        ("rows", "columns"),
        ("10", "10"),
        ("2", "10"),
        ("10", "2"),
    )

    def run(self, case=None, **kwargs):
        tags = self.get_tags(kwargs)
        for case in self.get_cases(case, kwargs):
            rows, columns = case
            f = self._get_benchmark_function(rows, columns)
            yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, rows, columns):
        return lambda: int(rows) * [int(columns) * [0]]
```

```
$ conbench example-cases --help
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
```

More case benchmark examples:

* [file_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/file_benchmark.py)
* [wide_dataframe_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/wide_dataframe_benchmark.py)
* [dataset_read_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/dataset_read_benchmark.py)
