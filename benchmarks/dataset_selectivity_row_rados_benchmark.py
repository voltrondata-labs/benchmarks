import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetRowSelectivityRadosBenchmark(_benchmark.Benchmark):
    """Read and filter a dataset."""

    name = "dataset-selectivity-row-rados"
    arguments = ["source"]
    sources = ["nyctaxi_2010-01"]
    sources_test = ["nyctaxi_sample"]
    valid_cases = (["selectivity"], ["1"], ["10"], ["25"], ["50"], ["75"], ["90"], ["99"], ["100"])

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            tags = self.get_tags(kwargs, source)
            #path = source.create_if_not_exists("parquet", "lz4")
            path = '/mnt/cephfs/nyc'
            dataset = pyarrow.dataset.dataset(path, format="rados-parquet")
            # array of percentage
            #selectivity_percentages = ["1", "10", "25", "50", "75", "90", "99", "100"] 
            for case in cases:
                (selectivity,) = case
                f = self._get_benchmark_function( dataset, selectivity)
                yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset, selectivity):
        filter_ = None
        
        if selectivity == "100":
            filter_ = None
        elif selectivity == "99":
            filter_ = (pyarrow.dataset.field("total_amount") > -200)
        elif selectivity == "90":
            filter_ = (pyarrow.dataset.field("total_amount") > 4)
        elif selectivity == "75":
            filter_ = (pyarrow.dataset.field("total_amount") > 9)
        elif selectivity == "50":
            filter_ = (pyarrow.dataset.field("total_amount") > 11)
        elif selectivity == "25":
            filter_ = (pyarrow.dataset.field("total_amount") > 19)
        elif selectivity == "10":
            filter_ = (pyarrow.dataset.field("total_amount") > 27)
        elif selectivity == "1":
            filter_ = (pyarrow.dataset.field("total_amount") > 69)

        return lambda: dataset.to_table(filter=filter_)
