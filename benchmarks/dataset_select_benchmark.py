import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetSelectBenchmark(_benchmark.Benchmark):
    """ Read and filter a dataset on partition expressions."""

    name = "dataset-select"
    arguments = ["source"]
    sources = ["nyctaxi_multi_parquet_s3_repartitioned"]
    # This does not load a large amount of data in tests because we
    # always pluck exactly one file from the dataset
    sources_test = ["nyctaxi_multi_parquet_s3_repartitioned"]
    options = {"cpu_count": {"type": int}}
    flags = {"cloud": True}

    def run(self, source, cpu_count=None, **kwargs):
        path_prefix = "ursa-labs-taxi-data-repartitioned-10k/"
        partitioning = pyarrow.dataset.DirectoryPartitioning.discover(
            field_names=["year", "month", "part"],
            infer_dictionary=True,
        )
        for source in self.get_sources(source):
            s3 = pyarrow.fs.S3FileSystem(region=source.region)
            dataset = pyarrow.dataset.dataset(
                source.paths,
                format="parquet",
                filesystem=s3,
                partitioning=partitioning,
                partition_base_dir=path_prefix,
            )
            f = self._get_benchmark_function(dataset)
            tags = self.get_tags(source, cpu_count)
            yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, dataset):
        year = pyarrow.dataset.field("year")
        month = pyarrow.dataset.field("month")
        part = pyarrow.dataset.field("part")
        filter_expr = (year == 2011) & (month == 1) & (part == 2)
        return lambda: dataset.to_table(filter=filter_expr)
