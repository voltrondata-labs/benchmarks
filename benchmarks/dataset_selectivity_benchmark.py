import conbench.runner
import pyarrow.dataset as ds

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetSelectivityBenchmark(_benchmark.Benchmark):
    """Read and filter a dataset with different selectivity."""

    name = "dataset-selectivity"
    sources = ["nyctaxi_multi_parquet_s3", "chi_traffic_2020_Q1"]
    sources_test = ["nyctaxi_multi_parquet_s3_sample", "chi_traffic_sample"]
    valid_cases = (["selectivity"], ["1%"], ["10%"], ["100%"])
    filters = {
        "nyctaxi_multi_parquet_s3": {
            "1%": ds.field("pickup_longitude") < -74.013451,  # 561384
            "10%": ds.field("pickup_longitude") < -74.002055,  # 5615432
            "100%": None,  # 56154689
        },
        "chi_traffic_2020_Q1": {
            "1%": ds.field("END_LONGITUDE") < -87.807262,  # 124530
            "10%": ds.field("END_LONGITUDE") < -87.7624,  # 1307565
            "100%": None,  # 13038291
        },
        "nyctaxi_multi_parquet_s3_sample": {
            "1%": ds.field("pickup_longitude") < -74.0124,  # 20
            "10%": ds.field("pickup_longitude") < -74.00172,  # 200
            "100%": None,  # 2000
        },
        "chi_traffic_sample": {
            "1%": ds.field("END_LONGITUDE") < -87.80726,  # 10
            "10%": ds.field("END_LONGITUDE") < -87.76148,  # 100
            "100%": None,  # 1000
        },
    }

    def run(self, source, case=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            source.download_source_if_not_exists()
            tags = self.get_tags(kwargs, source)
            format_str = "parquet"
            schema = ds.dataset(source.source_paths[0], format=format_str).schema
            for case in cases:
                (selectivity,) = case
                dataset = ds.dataset(
                    source.source_paths, schema=schema, format=format_str
                )
                f = self._get_benchmark_function(dataset, source.name, selectivity)
                yield self.benchmark(f, tags, kwargs, case)

    def _get_benchmark_function(self, dataset, source, selectivity):
        return lambda: dataset.to_table(
            filter=self.filters[source][selectivity]
        ).num_rows
