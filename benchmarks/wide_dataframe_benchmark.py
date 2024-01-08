import os
import pathlib

import conbenchlegacy.runner
import numpy
import pandas
import pyarrow
import pyarrow.parquet as parquet

from benchmarks import _benchmark, _sources


@conbenchlegacy.runner.register_benchmark
class WideDataframeBenchmark(_benchmark.Benchmark):
    """
    Read wide dataframe from parquet with pandas.
    See: https://issues.apache.org/jira/browse/ARROW-11469
    """

    name = "wide-dataframe"
    # 'use_legacy_dataset' used to be a meaningful benchmark parameter, but since that
    # behavior is deprecated we only keep it around to preserve benchmark history.
    valid_cases = (["use_legacy_dataset"], ["false"])

    def run(self, case=None, **kwargs):
        path = os.path.join(_sources.temp_dir, "wide.parquet")
        self._create_if_not_exists(path)

        for case in self.get_cases(case, kwargs):
            tags = self.get_tags(kwargs)
            f = self._get_benchmark_function(path)
            yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, path):
        return lambda: pandas.read_parquet(path)

    def _create_if_not_exists(self, path):
        if not pathlib.Path(path).exists():
            dataframe = pandas.DataFrame(numpy.random.rand(100, 10000))
            table = pyarrow.Table.from_pandas(dataframe)
            parquet.write_table(table, path)
