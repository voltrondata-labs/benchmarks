import os
import pathlib

import conbench.runner
import numpy
import pandas
import pyarrow
import pyarrow.parquet as parquet

from benchmarks import _benchmark, _sources


@conbench.runner.register_benchmark
class WideDataframeBenchmark(_benchmark.Benchmark):
    """
    Read wide dataframe from parquet with pandas.
    See: https://issues.apache.org/jira/browse/ARROW-11469

        WideDataframeBenchmark().run(options...)

    Parameters
    ----------
    all : boolean, optional
        Run all benchmark cases
    use_legacy_dataset : str, optional
        Valid values: "true", "false"
    case : sequence, optional
        Benchmark options as a sequence (rather than individual params):
        [<use_legacy_dataset>]
    cpu_count : int, optional
        Set the number of threads to use in parallel operations (arrow).
    iterations : int, default 1
        Number of times to run the benchmark.
    gc_collect : boolean, default True
        Whether to do garbage collection before each benchmark run.
    gc_disable : boolean, default True
        Whether to do disable collection during each benchmark run.
    run_id : str, optional
        Group executions together with a run id.
    run_name : str, optional
        Name of run (commit, pull request, etc).

    Returns
    -------
    (result, output) : sequence
        result : The benchmark result.
        output : The output from the benchmarked function.
    """

    name = "wide-dataframe"
    valid_cases = (
        ["use_legacy_dataset"],
        ["true"],
        ["false"],
    )
    options = {"cpu_count": {"type": int}}

    def run(self, case=None, cpu_count=None, **kwargs):
        path = os.path.join(_sources.temp_dir, "wide.parquet")
        self._create_if_not_exists(path)
        cases = self.get_cases(case, kwargs)

        for case in cases:
            (legacy,) = case
            # not using actual booleans... see hacks.py in conbench
            legacy = True if legacy == "true" else False
            tags = {"cpu_count": cpu_count}
            f = self._get_benchmark_function(path, legacy)
            yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, path, legacy):
        return lambda: pandas.read_parquet(path, use_legacy_dataset=legacy)

    def _create_if_not_exists(self, path):
        if not pathlib.Path(path).exists():
            dataframe = pandas.DataFrame(numpy.random.rand(100, 10000))
            table = pyarrow.Table.from_pandas(dataframe)
            parquet.write_table(table, path)
