import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetFilterBenchmark22(_benchmark.Benchmark):
    """Read and filter a dataset."""

    name = "dataset-selectivity-column-rados"
    arguments = ["source"]
    sources = ["nyctaxi_2010-01"]
    sources_test = ["nyctaxi_sample"]

    def run(self, source, **kwargs):
        for source in self.get_sources(source):
            #path = source.create_if_not_exists("parquet", "lz4")
            path = '/mnt/cephfs/nyc'
            dataset = pyarrow.dataset.dataset(path, format="rados-parquet")
            # array of number of columns
            col_selectivity = [1, 2, 5, 10, 14]
            f = self._get_benchmark_function( dataset, col_selectivity)
            tags = self.get_tags(kwargs, source)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset, col_selectivity):
        #columns = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'RatecodeID', 'store_and_fwd_flag', 'PULocationID', 'DOLocationID', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']
        columns = dataset.to_table().to_pandas().columns.values.tolist()
        
        # Default value for columns_
        columns_ = columns[:1]
        for per in col_selectivity:
            if per == "1":
                columns_ = columns[:1]
            if per == "2":
                columns_ = columns[:2]
            if per == "5":
                columns_ = columns[:5]
            if per == "10":
                columns_ = columns[:10]
            if per == "14":
                columns_ = columns[:14]
            
            dataset.to_table(columns=columns_)

        return lambda: dataset.to_table(columns=columns_)
