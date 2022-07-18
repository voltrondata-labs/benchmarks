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

    def run(self, source, **kwargs):
        for source in self.get_sources(source):
            path = source.create_if_not_exists("parquet", "lz4")
            dataset = pyarrow.dataset.dataset(
                path, schema=source.schema, format="parquet"
            )
            f = self._get_benchmark_function(dataset)
            tags = self.get_tags(kwargs, source)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset):
        # for this filter to work, source must be one of:
        #    nyctaxi_sample  --or--  nyctaxi_2010-01
        vendor = pyarrow.dataset.field("vendor_id")
        count = pyarrow.dataset.field("passenger_count")
        return lambda: dataset.to_table(filter=(vendor == "DDS") & (count > 3))
