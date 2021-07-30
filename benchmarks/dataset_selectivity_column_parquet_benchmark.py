import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetColumnSelectivityParquetBenchmark(_benchmark.Benchmark):
    """Read and filter a dataset."""

    name = "dataset-selectivity-column-parquet"
    arguments = ["source"]
    sources = ["nyctaxi_2010-01"]
    sources_test = ["nyctaxi_sample"]
    valid_cases = (["selectivity"], ["1"], ["2"], ["5"], ["10"], ["14"])

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            tags = self.get_tags(kwargs, source)
            #path = source.create_if_not_exists("parquet", "lz4")
            path = '/mnt/cephfs/nyc'
            dataset = pyarrow.dataset.dataset(path, format="parquet")
            for case in cases:
                (selectivity,) = case
                f = self._get_benchmark_function( dataset, selectivity)
                yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset, selectivity):
        columns = dataset.to_table().to_pandas().columns.values.tolist()
        
        # Default value for columns_
        columns_ = columns[:1]
        if selectivity == "1":
            columns_ = columns[:1]
        elif selectivity == "2":
            columns_ = columns[:2]
        elif selectivity == "5":
            columns_ = columns[:5]
        elif selectivity == "10":
            columns_ = columns[:10]
        elif selectivity == "14":
            columns_ = columns[:14]

        return lambda: dataset.to_table(columns=columns_)
