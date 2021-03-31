import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetFilterBenchmark(_benchmark.Benchmark):
    """Read and filter a dataset."""

    name = "dataset-filter"
    arguments = ["source"]
    sources = ["nyctaxi_2010-01"]
    sources_test = ["nyctaxi_sample"]
    options = {"cpu_count": {"type": int}}

    def run(self, source, cpu_count=None, **kwargs):
        for source in self.get_sources(source):
            path = source.create_if_not_exists("parquet", "lz4")
            dataset = pyarrow.dataset.dataset(path, format="parquet")
            f = self._get_benchmark_function(dataset)
            tags = self.get_tags(source, cpu_count)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset):
        # for this filter to work, source must be one of:
        #    nyctaxi_sample  --or--  nyctaxi_2010-01
        vendor = pyarrow.dataset.field("vendor_id")
        count = pyarrow.dataset.field("passenger_count")
        return lambda: dataset.to_table(filter=(vendor == "DDS") & (count > 3))
