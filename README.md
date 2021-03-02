# Apache Arrow Benchmarks


* [Setup](https://github.com/ursa-labs/benchmarks#setup)
* [Running benchmarks](https://github.com/ursa-labs/benchmarks#running-benchmarks)
* [Authoring benchmarks](https://github.com/ursa-labs/benchmarks#authoring-benchmarks)


## Setup


### Create workspace
    $ cd
    $ mkdir -p envs
    $ mkdir -p workspace


### Create virualenv
    $ cd ~/envs
    $ python3 -m venv qa
    $ source qa/bin/activate


### Clone repos
    (qa) $ cd ~/workspace/
    (qa) $ git clone https://github.com/ursa-labs/benchmarks.git
    (qa) $ git clone https://github.com/ursa-labs/conbench.git
    (qa) $ git clone https://github.com/apache/arrow.git


### Install benchmarks dependencies
    (qa) $ cd ~/workspace/benchmarks/
    (qa) $ pip install -r requirements-test.txt
    (qa) $ pip install -r requirements-build.txt
    (qa) $ python setup.py develop


### Install arrowbench (to run R benchmarks)
    $ R
    > install.packages('remotes')
    > remotes::install_github("ursa-labs/arrowbench")


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

    (qa) $ cd ~/workspace/benchmarks/benchmarks/
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
        modified:   foo.py
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

    (qa) $ cd ~/workspace/benchmarks/benchmarks/


    (qa) $ conbench --help
    Usage: conbench [OPTIONS] COMMAND [ARGS]...

      Conbench: Language-independent Continuous Benchmarking (CB) Framework

    Options:
      --help  Show this message and exit.

    Commands:
      compare           Compare benchmark runs.
      cpp-micro         Run the Arrow C++ micro benchmarks.
      csv-read          Run csv-read benchmark.
      dataset-filter    Run dataset-filter benchmark.
      example-external  Run example-external benchmark.
      example-simple    Run example-simple benchmark.
      file-read         Run file-read benchmark(s).
      file-write        Run file-write benchmark(s).
      list              List available benchmarks commands by language.


    (qa) $ conbench file-write --help
    Usage: conbench file-write [OPTIONS] SOURCE

      Run file-write benchmark(s).

      For each benchmark option, the first option value is the default.

      Valid benchmark combinations:
      --file-type=parquet --compression=uncompressed --input-type=table
      --file-type=parquet --compression=uncompressed --input-type=dataframe
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
      --all BOOLEAN                   [default: false]
      --file-type [feather|parquet]
      --compression [uncompressed|snappy|lz4|zstd]
      --input-type [table|dataframe]
      --cpu-count INTEGER             [default: 1]
      --iterations INTEGER            [default: 1]
      --gc-collect BOOLEAN            [default: true]
      --gc-disable BOOLEAN            [default: true]
      --show-result BOOLEAN           [default: true]
      --show-output BOOLEAN           [default: false]
      --run-id TEXT                   Group executions together with a run id.
      --help                          Show this message and exit.


    (qa) $ conbench file-read nyctaxi_sample --file-type=parquet \
      --iterations=10 --gc-disable=false
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

These example benchmarks and their tests can be found here:

* [_example_benchmarks.py](https://github.com/ursa-labs/benchmarks/blob/main/benchmarks/_example_benchmarks.py)
* [test_example_benchmarks.py](https://github.com/ursa-labs/benchmarks/blob/main/benchmarks/tests/test_example_benchmarks.py)


### Example simple benchmark

Other benchmarks that have minimal scaffolding:

* [csv_read_benchmark.py](https://github.com/ursa-labs/benchmarks/blob/main/benchmarks/csv_read_benchmark.py)
* [dataset_filter_benchmark.py](https://github.com/ursa-labs/benchmarks/blob/main/benchmarks/dataset_filter_benchmark.py)


```python
@conbench.runner.register_benchmark
class SimpleBenchmark(_benchmark.Benchmark):
    """Example benchmark with no source, cases, or options.

    $ conbench example-simple --help
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

    name = "example-simple"

    def run(self, **kwargs):
        tags = {"year": "2020"}
        f = self._get_benchmark_function()
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self):
        return lambda: print("hello!")
```


### Example external benchmark

Also see this actual external benchmark:

* [cpp_micro_benchmarks.py](https://github.com/ursa-labs/benchmarks/blob/main/benchmarks/cpp_micro_benchmarks.py)


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
      --help                 Show this message and exit.
    """

    name = "example-external"
    external = True

    def run(self, **kwargs):
        tags = {"year": "2020"}
        context = {"benchmark_language": "C++"}

        # external results from somewhere
        data, unit = [100, 200, 300], "i/s"
        times, time_unit = [0.100, 0.300, 0.300], "s"

        yield self.record(
            tags,
            context,
            data,
            unit,
            times,
            time_unit,
            kwargs,
            output=data,
        )
```


### Example cases benchmark

Also see this actual benchmark with many cases, options, and a source:

* [file_benchmark.py](https://github.com/ursa-labs/benchmarks/blob/main/benchmarks/file_benchmark.py)


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
      --gc-collect BOOLEAN         [default: true]
      --gc-disable BOOLEAN         [default: true]
      --show-result BOOLEAN        [default: true]
      --show-output BOOLEAN        [default: false]
      --run-id TEXT                Group executions together with a run id.
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
