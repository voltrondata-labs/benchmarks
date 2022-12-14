import itertools
import logging
import os
import shutil
import time
import uuid

import conbench.runner
import pyarrow
import pyarrow.dataset as ds
from pyarrow.dataset import Dataset

from benchmarks import _benchmark

log = logging.getLogger(__name__)


@conbench.runner.register_benchmark
class DatasetSerializeBenchmark(_benchmark.Benchmark):
    """
    This benchmark is supposed to measure the time it takes to write data from
    memory (from a pyarrow Table) to a tmpfs file system, given a specific
    serialization format (parquet, arrow, ...).

    To make this benchmark agnostic to disk read performance on the input side
    of things, the data is read fully into memory before starting (and timing)
    the benchmark function. That is (believed to be) achieved with:

        data = source_dataset.to_table(filter=somefilter)

    After data of interest has been read into memory, the following call is
    used for both serialization and writing-to-filesystem in one go:

        pyarrow.dataset.write_dataset(format=someformat)

    That operation is timed (and the duraiton is the major output of this
    benchmark).

    The data is written to `/dev/shm` (available on all Linux systems). This is
    a file system backed by RAM (tmpfs). The assumption is that writing to
    tmpfs is fast, and most importantly _stable_.

    This benchmark does not resolve how much time goes into the CPU work for
    serialization vs. the system calls for writing to tmpfs (that would be a
    different question to answer, and interesting one, that is maybe more of a
    task for profiling).

    There are two dimensions that are varied:

        - serialization format
        - amount of the data being written, as set by a filter on the input
    """

    name = "dataset-serialize"

    arguments = ["source"]

    sources = [
        "nyctaxi_multi_parquet_s3",
        # "nyctaxi_multi_ipc_s3",
        # "chi_traffic_2020_Q1",
    ]

    sources_test = [
        "nyctaxi_multi_parquet_s3_sample",
        # "nyctaxi_multi_ipc_s3_sample",
        # "chi_traffic_sample",
    ]

    _params = {
        "selectivity": ("1pc", "10pc"),
        "format": (
            "parquet",
            "arrow",
            "ipc",
            "feather",
            "csv",
        ),
    }

    valid_cases = [tuple(_params.keys())] + list(
        itertools.product(*[v for v in _params.values()])
    )

    filters = {
        "nyctaxi_multi_parquet_s3": {
            "1pc": ds.field("pickup_longitude") < -74.013451,  # 561384
            "10pc": ds.field("pickup_longitude") < -74.002055,  # 5615432
            "100pc": None,  # 56154689
        },
        "nyctaxi_multi_ipc_s3": {
            "1pc": ds.field("pickup_longitude") < -74.014053,  # 596165
            "10pc": ds.field("pickup_longitude") < -74.002708,  # 5962204
            "100pc": None,  # 59616487
        },
        "chi_traffic_2020_Q1": {
            "1pc": ds.field("END_LONGITUDE") < -87.807262,  # 124530
            "10pc": ds.field("END_LONGITUDE") < -87.7624,  # 1307565
            "100pc": None,  # 13038291
        },
        **dict.fromkeys(
            ["nyctaxi_multi_parquet_s3_sample", "nyctaxi_multi_ipc_s3_sample"],
            {
                "1pc": ds.field("pickup_longitude") < -74.0124,  # 20
                "10pc": ds.field("pickup_longitude") < -74.00172,  # 200
                "100pc": None,  # 2000
            },
        ),
        "chi_traffic_sample": {
            "1pc": ds.field("END_LONGITUDE") < -87.80726,  # 10
            "10pc": ds.field("END_LONGITUDE") < -87.76148,  # 100
            "100pc": None,  # 1000
        },
    }

    _case_tmpdir_mapping = {}

    def _create_tmpdir_in_ramdisk(self, case: tuple):
        # Build simple prefix string to facilitate correlating directory names
        # to test cases.
        pfx = "-".join(c.lower()[:9] for c in case)
        dirpath = os.path.join("/dev/shm", pfx + "-" + str(uuid.uuid4()))

        self._case_tmpdir_mapping[tuple(case)] = dirpath

        os.makedirs(dirpath, exist_ok=False)
        return dirpath

    def _get_dataset_for_source(self, source) -> Dataset:
        """Helper to construct a Dataset object."""

        return pyarrow.dataset.dataset(
            source.source_paths,
            schema=pyarrow.dataset.dataset(
                source.source_paths[0], format=source.format_str
            ).schema,
            format=source.format_str,
        )

    def run(self, source, case=None, **kwargs):

        cases = self.get_cases(case, kwargs)

        for source in self.get_sources(source):

            log.info("source %s: download, if required", source.name)
            source.download_source_if_not_exists()
            tags = self.get_tags(kwargs, source)

            t0 = time.monotonic()
            source_ds = self._get_dataset_for_source(source)
            log.info(
                "constructed Dataset object for source in %.4f s", time.monotonic() - t0
            )

            for case in cases:

                log.info("case %s: create directory", case)
                dirpath = self._create_tmpdir_in_ramdisk(case)
                log.info("directory created, path: %s", dirpath)

                yield self.benchmark(
                    f=self._get_benchmark_function(
                        case, source.name, source_ds, dirpath
                    ),
                    extra_tags=tags,
                    options=kwargs,
                    case=case,
                )

                # Free up memory in the RAM disk (tmpfs), assuming that we're
                # otherwise getting close to filling it (depending on the
                # machine this is executed on, a single test might easily
                # occupy 10 % or more of this tmpfs).
                log.info("removing directory: %s", dirpath)
                shutil.rmtree(dirpath)

    def _get_benchmark_function(
        self, case, source_name: str, source_ds: Dataset, dirpath: str
    ):

        (selectivity, serialization_format) = case

        # Option A: read-from-disk -> deserialize -> filter -> into memory
        # before timing serialize -> write-to-tmpfs
        t0 = time.monotonic()
        data = source_ds.to_table(filter=self.filters[source_name][selectivity])
        log.info("read source dataset into memory in %.4f s", time.monotonic() - t0)

        # Option B (thrown away, but kept for posterity): use a Scanner()
        # object to transparently filter the source dataset upon consumption,
        # in which case what's timed is read-from-disk -> deserialize -> filter
        # -> serialize -> write-to-tmpfs
        #
        # Note(JP): I have confirmed that for the data used in this benchmark
        # this option is dominated by read-from-disk to the extent that no
        # useful signal is generated for the write phase, at least for some
        # serialization formats.
        #
        # data = pyarrow.dataset.Scanner.from_dataset(source_ds,
        #     filter=self.filters[source_name][selectivity])

        def benchfunc():
            # This is a hack to make each iteration work in a separate
            # directory (otherwise some write operations would error out saying
            # that the target directory is not empty). With `benchrun` it will
            # be easier to cleanly hook into doing resource management before
            # and after an iteration w/o affecting the timing measurement.
            # Assume that creating a directory does not significantly add to
            # the duration of the actual payload function.
            dp = os.path.join(dirpath, str(uuid.uuid4())[:8])
            os.makedirs(dp)

            # When dimensioning of benchmark parameters and execution
            # environment are not adjusted to each other, tmpfs quickly gets
            # full. In that case writing might fail with
            #
            #   File "pyarrow/_dataset.pyx", line 2859, in
            #   pyarrow._dataset._filesystemdataset_write File
            #   "pyarrow/error.pxi", line 113, in pyarrow.lib.check_status
            #   OSError: [Errno 28] Error writing bytes to file. Detail: [errno
            #   28] No space left on device

            pyarrow.dataset.write_dataset(
                data=data, format=serialization_format, base_dir=dp
            )

            # The benchmark function returns `None` for now. If we need
            # deeper inspection into the result maybe iterate on that.

        return benchfunc
