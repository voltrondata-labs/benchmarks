import conbench.runner
import pyarrow.dataset as ds

from benchmarks._benchmark import Benchmark


def run_get_file_info(dataset_uri):
    ds.dataset(dataset_uri, format="parquet")


@conbench.runner.register_benchmark
class GetFileInfoBenchmark(Benchmark):
    """Recursively list all files"""

    name = "recursive-get-file-info"
    valid_cases = (
        ["dataset_uri"],
        ["s3://ursa-qa/wide-partition"],
        ["s3://ursa-qa/flat-partition"],
    )

    def run(self, case=None, **kwargs):
        for case in self.get_cases(case, kwargs):
            (dataset_uri,) = case
            tags = self.get_tags(kwargs)
            f = lambda: run_get_file_info(dataset_uri)
            yield self.benchmark(f, tags, kwargs, case)
