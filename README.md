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


### Running tests
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ pytest -vv benchmarks/tests/


### Formatting code (before committing)
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ git status
        modified: foo.py
    (qa) $ black foo.py
        reformatted foo.py
    (qa) $ git add foo.py


### Generating a coverage report
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

    (qa) $ conbench --help
    Usage: conbench [OPTIONS] COMMAND [ARGS]...

      Conbench: Language-independent Continuous Benchmarking (CB) Framework

    Options:
      --help  Show this message and exit.

    Commands:
      cpp-micro                 Run the Arrow C++ micro benchmarks.
      csv-read                  Run csv-read benchmark.
      dataframe-to-table        Run dataframe-to-table benchmark.
      dataset-filter            Run dataset-filter benchmark.
      dataset-read              Run dataset-read benchmark(s).
      dataset-select            Run dataset-select benchmark.
      example-R-only            Run example-R-only benchmark.
      example-R-only-exception  Run example-R-only-exception benchmark.
      example-R-only-no-result  Run example-R-only-no-result benchmark.
      example-cases             Run example-cases benchmark(s).
      example-cases-exception   Run example-cases-exception benchmark(s).
      example-external          Run example-external benchmark.
      example-simple            Run example-simple benchmark.
      example-simple-exception  Run example-simple-exception benchmark.
      file-read                 Run file-read benchmark(s).
      file-write                Run file-write benchmark(s).
      list                      List of benchmarks (for orchestration).
      wide-dataframe            Run wide-dataframe benchmark(s).


Help is also available for individual benchmark commands.

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
      --all BOOLEAN                   [default: false]
      --language [Python|R]
      --cpu-count INTEGER
      --iterations INTEGER            [default: 1]
      --drop-caches BOOLEAN           [default: false]
      --gc-collect BOOLEAN            [default: true]
      --gc-disable BOOLEAN            [default: true]
      --show-result BOOLEAN           [default: true]
      --show-output BOOLEAN           [default: false]
      --run-id TEXT                   Group executions together with a run id.
      --run-name TEXT                 Name of run (commit, pull request, etc).
      --help                          Show this message and exit.

Example benchmark execution.

    (qa) $ conbench file-read nyctaxi_sample --file-type=parquet --iterations=10 --gc-disable=false
    {
        "context": {
            "arrow_compiler_flags": "-fPIC -arch x86_64 -arch x86_64 -std=c++11 -Qunused-arguments -fcolor-diagnostics -O3 -DNDEBUG",
            "arrow_compiler_id": "AppleClang",
            "arrow_compiler_version": "11.0.0.11000033",
            "arrow_git_revision": "478286658055bb91737394c2065b92a7e92fb0c1",
            "arrow_version": "2.0.0",
            "benchmark_language": "Python",
            "benchmark_language_version": "Python 3.8.5"
        },
        "machine_info": {
            "architecture_name": "x86_64",
            "cpu_l1d_cache_bytes": "32768",
            "cpu_l1i_cache_bytes": "32768",
            "cpu_l2_cache_bytes": "262144",
            "cpu_l3_cache_bytes": "4194304",
            "cpu_core_count": "2",
            "cpu_frequency_max_hz": "3500000000",
            "cpu_model_name": "Intel(R) Core(TM) i7-7567U CPU @ 3.50GHz",
            "cpu_thread_count": "4",
            "kernel_name": "19.6.0",
            "memory_bytes": "17179869184",
            "name": "diana",
            "os_name": "macOS",
            "os_version": "10.15.7"
        },
        "stats": {
            "batch_id": "7b2fdd9f929d47b9960152090d47f8e6",
            "run_id": "b00966bd99a94c34abc7a042b7a0a5b4",
            "data": [
                "0.083157",
                "0.003450",
                "0.004124",
                "0.010661",
                "0.003458",
                "0.003102",
                "0.006444",
                "0.004806",
                "0.008307",
                "0.003677"
            ],
            "unit": "s",
            "data": [],
            "time_unit": "s",
            "iqr": "0.004328",
            "iterations": 10,
            "max": "0.083157",
            "mean": "0.013119",
            "median": "0.004465",
            "min": "0.003102",
            "q1": "0.003513",
            "q3": "0.007841",
            "stdev": "0.024733",
            "timestamp": "2020-11-25T21:02:42.706806+00:00"
        },
        "tags": {
            "action": "read",
            "compression": "uncompressed",
            "cpu_count": 1,
            "dataset": "nyctaxi_sample",
            "file_type": "parquet",
            "gc_collect": true,
            "gc_disable": false,
            "name": "file-read",
            "output_type": "table"
        }
    }


### Manually run benchmarks
    (qa) $ cd ~/workspace/benchmarks/benchmarks/
    (qa) $ python
    >>> import json
    >>> from benchmarks import file_benchmark
    >>> from benchmarks import _sources
    >>> source = _sources.Source("nyctaxi_sample")
    >>> benchmark = file_benchmark.FileWriteBenchmark()
    >>> [(result, output)] = benchmark.run(
    ...     source,
    ...     file_type="parquet",
    ...     compression="snappy",
    ...     input_type="table",
    ...     iterations=5,
    ...     cpu_count=2,
    ...     gc_collect=True,
    ...     gc_disable=True
    ... )
    >>> print(json.dumps(result, indent=4, sort_keys=True))
    {
        "context": {
            "arrow_compiler_flags": "-fPIC -arch x86_64 -arch x86_64 -std=c++11 -Qunused-arguments -fcolor-diagnostics -O3 -DNDEBUG",
            "arrow_compiler_id": "AppleClang",
            "arrow_compiler_version": "11.0.0.11000033",
            "arrow_git_revision": "478286658055bb91737394c2065b92a7e92fb0c1",
            "arrow_version": "2.0.0",
            "benchmark_language": "Python",
            "benchmark_language_version": "Python 3.8.5"
        },
        "machine_info": {
            "architecture_name": "x86_64",
            "cpu_l1d_cache_bytes": "32768",
            "cpu_l1i_cache_bytes": "32768",
            "cpu_l2_cache_bytes": "262144",
            "cpu_l3_cache_bytes": "4194304",
            "cpu_core_count": "2",
            "cpu_frequency_max_hz": "3500000000",
            "cpu_model_name": "Intel(R) Core(TM) i7-7567U CPU @ 3.50GHz",
            "cpu_thread_count": "4",
            "kernel_name": "19.6.0",
            "memory_bytes": "17179869184",
            "name": "diana",
            "os_name": "macOS",
            "os_version": "10.15.7"
        },
        "stats": {
            "batch_id": "7b2fdd9f929d47b9960152090d47f8e6",
            "run_id": "b00966bd99a94c34abc7a042b7a0a5b4",
            "data": [
                "0.099094",
                "0.037129",
                "0.036381",
                "0.148896",
                "0.008104",
                "0.005496",
                "0.009871",
                "0.006008",
                "0.007978",
                "0.004733"
            ],
            "unit": "s",
            "data": [],
            "time_unit": "s",
            "iqr": "0.030442",
            "iterations": 10,
            "max": "0.148896",
            "mean": "0.036369",
            "median": "0.008988",
            "min": "0.004733",
            "q1": "0.006500",
            "q3": "0.036942",
            "stdev": "0.049194",
            "timestamp": "2020-11-25T21:02:42.706806+00:00"
        },
        "tags": {
            "action": "write",
            "compression": "snappy",
            "cpu_count": 2,
            "dataset": "nyctaxi_sample",
            "file_type": "parquet",
            "gc_collect": true,
            "gc_disable": true,
            "input_type": "table",
            "name": "file-write"
        }
    }
    >>>


## Authoring benchmarks

There are three main types of benchmarks: "simple benchmarks" that time
the execution of a unit of work, "external benchmarks" that just record
benchmark results that were obtained from some other benchmarking tool,
and "case benchmarks" which time the execution of a unit of work under
different scenarios.

Included in this repository are contrived, minimal examples of these
different kinds of benchmarks to be used as templates for benchmark
authoring. These example benchmarks and their tests can be found here:

* [_example_benchmarks.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/_example_benchmarks.py)
* [test_example_benchmarks.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/tests/test_example_benchmarks.py)


### Example simple benchmarks

Note that this benchmark extends `benchmarks._benchmark.Benchmark`,
implements the minimum required `run()` method, and registers itself
with the `@conbench.runner.register_benchmark` decorator.

```python
@conbench.runner.register_benchmark
class SimpleBenchmark(_benchmark.Benchmark):
    """Example benchmark with no source, cases, or options.

    $ conbench example-simple --help
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

    name = "example-simple"

    def run(self, **kwargs):
        tags = {"year": "2020"}
        f = self._get_benchmark_function()
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self):
        return lambda: print("hello!")
```


More simple benchmark examples that have minimal scaffolding:

* [csv_read_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/csv_read_benchmark.py)
* [dataset_filter_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/dataset_filter_benchmark.py)


### Example external benchmarks

Note that the following benchmark sets `external = True`, and calls
`record()` rather than `benchmark()` as the example above does.

```python
@conbench.runner.register_benchmark
class RecordExternalBenchmark(_benchmark.Benchmark):
    """Example benchmark that just records external results.

    $ conbench example-external --help
    Usage: conbench example-external [OPTIONS]

      Run example-external benchmark.

    Options:
      --show-result BOOLEAN  [default: true]
      --show-output BOOLEAN  [default: false]
      --run-id TEXT          Group executions together with a run id.
      --run-name TEXT        Name of run (commit, pull request, etc).
      --help                 Show this message and exit.
    """

    name = "example-external"
    external = True

    def run(self, **kwargs):
        tags = {"year": "2020"}
        context = {"benchmark_language": "C++"}

        # external results from somewhere
        result = {
            "data": [100, 200, 300],
            "unit": "i/s",
            "times": [0.100, 0.200, 0.300],
            "time_unit": "s",
        }

        yield self.record(
            result,
            tags,
            context,
            kwargs,
            output=result["data"],
        )
```

Note that the following benchmark extends `BenchmarkR`, sets`r_only` to
`True`, defines `r_name`, implements `_get_r_command()`, and calls
`r_benchmark()` rather than `benchmark()` or `record()`.

```python
@conbench.runner.register_benchmark
class WithoutPythonBenchmark(_benchmark.Benchmark, _benchmark.BenchmarkR):
    """Example R benchmark that doesn't have a Python equivalent.

    $ conbench example-R-only --help
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

    r_only = True
    name, r_name = "example-R-only", "placebo"

    def run(self, **kwargs):
        tags = {"year": "2020", "cpu_count": kwargs.get("cpu_count")}
        command = self._get_r_command(kwargs)
        yield self.r_benchmark(command, tags, kwargs)

    def _get_r_command(self, options):
        return (
            f"library(arrowbench); "
            f"run_one(arrowbench:::{self.r_name}, "
            f'n_iter={options.get("iterations", 1)}, '
            f"cpu_count={self.r_cpu_count(options)})"
        )
```


More external benchmark examples that record C++ and R benchmark results:

* [cpp_micro_benchmarks.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/cpp_micro_benchmarks.py)
* [dataframe_to_table_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/dataframe_to_table_benchmark.py)


### Example case benchmarks

Note that the following benchmark declares the valid combinations
in `valid_cases`, which reads like a CSV (the first row contains the
cases names). This benchmark example also accepts a data source
argument (see `arguments`), and additional `options` that re reflected
in the resulting command line interface.


```python
@conbench.runner.register_benchmark
class CasesBenchmark(_benchmark.Benchmark):
    """Example benchmark with a source, cases, and an option (count).

    $ conbench example-cases --help
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
      --all BOOLEAN                [default: false]
      --color [pink|yellow|green]
      --fruit [apple|orange]
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

    name = "example-cases"
    valid_cases = (
        ("color", "fruit"),
        ("pink", "apple"),
        ("yellow", "apple"),
        ("green", "apple"),
        ("yellow", "orange"),
        ("pink", "orange"),
    )
    arguments = ["source"]
    options = {"count": {"default": 1, "type": int}}

    def run(self, dataset, case=None, count=1, **kwargs):
        if not isinstance(source, _sources.Source):
            source = _sources.Source(source)

        cases = self.get_cases(case, kwargs)
        tags = self._get_tags(source, count)
        for case in cases:
            f = self._get_benchmark_function(source, count, case)
            yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, source, count, case):
        return lambda: print(count * f"{source.name}, {case}\n")

    def _get_tags(self, source, count):
        info = {"count": count}
        return {**source.tags, **info}
```

More case benchmark examples:

* [file_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/file_benchmark.py)
* [wide_dataframe_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/wide_dataframe_benchmark.py)
* [dataset_read_benchmark.py](https://github.com/ursacomputing/benchmarks/blob/main/benchmarks/dataset_read_benchmark.py)
