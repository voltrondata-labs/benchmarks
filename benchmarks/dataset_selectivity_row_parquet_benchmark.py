import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetRowSelectivityParquetBenchmark(_benchmark.Benchmark):
    """Read and filter a dataset."""

    name = "dataset-selectivity-row-parquet"
    arguments = ["source"]
    sources = ["nyctaxi_2010-01"]
    sources_test = ["nyctaxi_sample"]

    def run(self, source, **kwargs):
        for source in self.get_sources(source):
            #path = source.create_if_not_exists("parquet", "lz4")
            path = '/mnt/cephfs/nyc'
            dataset = pyarrow.dataset.dataset(path, format="parquet")
            # array of percentage
            selectivity_percentages = ["1", "10", "25", "50", "75", "90", "99", "100"] 
            f = self._get_benchmark_function( dataset, selectivity_percentages)
            tags = self.get_tags(kwargs, source)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset, selectivity_percentages):
        filter_ = None
        for per in selectivity_percentages:
            if per == "100":
                filter_ = None
            if per == "99":
                filter_ = (pyarrow.dataset.field("total_amount") > -200)
            if per == "90":
                filter_ = (pyarrow.dataset.field("total_amount") > 4)
            if per == "75":
                filter_ = (pyarrow.dataset.field("total_amount") > 9)
            if per == "50":
                filter_ = (pyarrow.dataset.field("total_amount") > 11)
            if per == "25":
                filter_ = (pyarrow.dataset.field("total_amount") > 19)
            if per == "10":
                filter_ = (pyarrow.dataset.field("total_amount") > 27)
            if per == "1":
                filter_ = (pyarrow.dataset.field("total_amount") > 69)
            
            dataset.to_table(filter=filter_)

        return lambda: dataset.to_table(filter=filter_)
