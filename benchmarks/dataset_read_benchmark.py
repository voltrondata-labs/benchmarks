import conbench.runner
import pyarrow
import pyarrow.dataset
import pyarrow.fs

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class DatasetReadBenchmark(_benchmark.Benchmark):
    """
    Read many S3 parquet files into an arrow table.

        DatasetReadBenchmark().run(<source>, options...)

    Parameters
    ----------
    source : str
        A source name from the benchmarks source store.
    all : boolean, optional
        Run all benchmark cases
    pre_buffer : str, optional
        Valid values: "true", "false"
    case : sequence, optional
        Benchmark options as a sequence (rather than individual params):
        [<pre_buffer>]
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

    name = "dataset-read"
    valid_cases = (
        ["pre_buffer"],
        ["true"],
        ["false"],
    )
    arguments = ["source"]
    sources = ["nyctaxi_multi_parquet_s3"]
    sources_test = ["nyctaxi_multi_parquet_s3_sample"]
    options = {"cpu_count": {"type": int}}

    def run(self, source, case=None, cpu_count=None, **kwargs):
        cases = self.get_cases(case, kwargs)
        for source in self.get_sources(source):
            tags = self.get_tags(source, cpu_count)
            schema = self._get_schema(source)
            s3 = pyarrow.fs.S3FileSystem(region=source.region)
            for case in cases:
                (pre_buffer,) = case
                legacy, parquet = self._get_format(pre_buffer)
                case = (None,) if legacy else case
                data = pyarrow.dataset.FileSystemDataset.from_paths(
                    source.paths,
                    schema=schema,
                    format=parquet,
                    filesystem=s3,
                )
                f = self._get_benchmark_function(data)
                yield self.benchmark(f, tags, kwargs, case)
                if legacy:
                    break  # no need to run the null legacy case twice

    def _get_benchmark_function(self, data):
        return lambda: data.to_table()

    def _get_schema(self, source):
        # TODO: FileSystemDataset.from_paths() can't currently discover
        # the schema, but pyarrow.dataset.dataset() can. Ideally, we
        # would just be able to omit the schema in FileSystemDataset.
        path = "s3://" + source.paths[0]
        return pyarrow.dataset.dataset(path, format="parquet").schema

    def _get_format(self, pre_buffer):
        # not using actual booleans... see hacks.py in conbench
        pre_buffer = True if pre_buffer == "true" else False
        read_options = {"pre_buffer": pre_buffer}
        parquet_format = pyarrow.dataset.ParquetFileFormat
        try:
            return False, parquet_format(read_options=read_options)
        except TypeError:
            return True, parquet_format(read_options={})
