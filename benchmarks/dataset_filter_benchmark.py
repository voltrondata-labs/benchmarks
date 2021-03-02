import conbench.runner
import pyarrow.dataset

from benchmarks import _benchmark, _sources


@conbench.runner.register_benchmark
class DatasetFilterBenchmark(_benchmark.Benchmark):
    """
    Read and filter a dataset.

        DatasetFilterBenchmark().run(<source>, options...)

    Parameters
    ----------
    source : str
        A source name from the benchmarks source store.
    cpu_count : int, optional
        Set the number of threads to use in parallel operations (arrow).
    iterations : int, default 1
        Number of times to run the benchmark.
    gc_collect : boolean, default True
        Whether to do garbage collection before each benchmark run.
    gc_disable : boolean, default True
        Whether to do disable collection during each benchmark run.
    run_id : str, optional
        Group executions together with a run id.

    Returns
    -------
    (result, output) : sequence
        result : The benchmark result.
        output : The output from the benchmarked function.
    """

    name = "dataset-filter"
    arguments = ["source"]
    options = {"cpu_count": {"type": int}}

    def run(self, source, cpu_count=None, **kwargs):
        if not isinstance(source, _sources.Source):
            source = _sources.Source(source)

        path = source.create_if_not_exists("parquet", "lz4")
        data = pyarrow.dataset.dataset(path, format="parquet")

        f = self._get_benchmark_function(data)
        tags = self.get_tags(source, cpu_count)
        yield self.benchmark(f, tags, kwargs)

    def _get_benchmark_function(self, data):
        # for this filter to work, source must be one of:
        #    nyctaxi_sample  --or--  nyctaxi_2010-01
        vendor = pyarrow.dataset.field("vendor_id")
        count = pyarrow.dataset.field("passenger_count")
        return lambda: data.to_table(filter=(vendor == "DDS") & (count > 3))
