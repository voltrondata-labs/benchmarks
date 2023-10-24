import itertools
import logging
import os
import shutil
import subprocess
import sys
import time
import uuid

import conbenchlegacy.runner
import pyarrow
import pyarrow.dataset as ds

from benchmarks import _benchmark

log = logging.getLogger(__name__)


# All case permutations write below /dev/shm/<SHM_DIR_PREFIX>. That directory
# tree is removed upon completion, not always in case of error though.
OUTPUT_DIR_PREFIX = os.path.join("/dev/shm/", "bench-" + str(uuid.uuid4())[:8])


@conbenchlegacy.runner.register_benchmark
class DatasetSerializeBenchmark(_benchmark.Benchmark):
    """
    This benchmark is supposed to measure the time it takes to write data from
    memory (from a pyarrow Table) to a tmpfs file system, given a specific
    serialization format (parquet, arrow, ...).

    To make this benchmark agnostic to disk read performance on the input side
    of things, the data is read fully into memory before starting (and timing)
    the benchmark function. That is (believed to be) achieved with:

        data = source_dataset.to_table(filter=somefilter, ...)

    After data of interest has been read into memory, the following call is
    used for both serialization and writing-to-filesystem in one go:

        pyarrow.dataset.write_dataset(format=someformat, ...)

    That operation is timed (and the duration is the major output of this
    benchmark).

    The data is written to `/dev/shm` (available on all Linux systems). This is
    a file system backed by RAM (tmpfs). The assumption is that writing to
    tmpfs is fast (so fast that benchmark duration is significantly affected by
    the serialization itself), and stable (so that its performance is ~constant
    across runs on the same machine).

    This benchmark does not resolve how much time goes into the CPU work for
    serialization vs. the system calls for writing to tmpfs (that would be a
    different question to answer, an interesting one, that is maybe more of a
    task for profiling).

    There are two dimensions that are varied:

        - serialization format
        - amount of the data being written, as set by a filter on the input

    A note about /dev/shm: it's of great value because

    - unprivileges users can write to it
    - the `base_dir` arg to pyarrow.dataset.write_dataset() requires a path to
      a directory. That is, one cannot inject a memory-backed Python file
      object (a strategy that's elsewhere often used to simulate writing to an
      actual file)
    - it is not available on MacOS which is why we skip this test

    """

    name = "dataset-serialize"

    iterations = 6

    arguments = ["source"]

    sources = [
        "nyctaxi_multi_parquet_s3",
        "nyctaxi_multi_ipc_s3",
        # "chi_traffic_2020_Q1",
    ]

    sources_test = [
        "nyctaxi_multi_parquet_s3_sample",
        "nyctaxi_multi_ipc_s3_sample",
        "chi_traffic_sample",
    ]

    _params = {
        "selectivity": ("1pc", "10pc", "100pc"),
        "format": (
            "parquet",
            "arrow",
            # Writing ORC? Is this supposed to work?
            # So far, it segfaults: https://github.com/apache/arrow/issues/14968.
            # "orc",
            "feather",
            "csv",
        ),
    }

    valid_cases = [tuple(_params.keys())] + list(
        itertools.product(*[v for v in _params.values()])
    )

    # Define number of rows to pick from relevant data sets to veeery roughly
    # meet the 1 %, 10 % cases.
    _get_n_rows = {
        # rows in dataset: 56154689
        "nyctaxi_multi_parquet_s3": {
            "1pc": 561000,
            "10pc": 5610000,
        },
        # rows in dataset: 59616487
        "nyctaxi_multi_ipc_s3": {
            "1pc": 596000,
            "10pc": 5960000,
        },
        # rows in dataset: 13038291
        "chi_traffic_2020_Q1": {
            "1pc": 130000,
            "10pc": 1300000,
        },
    }

    _case_tmpdir_mapping = {}

    def _create_tmpdir_in_ramdisk(self, case: tuple):
        # Build simple prefix string for specific test case to facilitate
        # correlating directory names to test cases.
        pfx = "-".join(c.lower()[:9] for c in case)
        dirpath = os.path.join(OUTPUT_DIR_PREFIX, pfx + "-" + str(uuid.uuid4()))

        self._case_tmpdir_mapping[tuple(case)] = dirpath

        os.makedirs(dirpath, exist_ok=False)
        return dirpath

    def _get_dataset_for_source(self, source) -> ds.Dataset:
        """Helper to construct a Dataset object."""

        return pyarrow.dataset.dataset(
            source.source_paths,
            schema=pyarrow.dataset.dataset(
                source.source_paths[0], format=source.format_str
            ).schema,
            format=source.format_str,
        )

    def _report_dirsize_and_wipe(self, dirpath: str):
        """
        This module already has a dependency on Linux so we can just as well
        spawn `du` for correct recursive directory size reporting"""

        ducmd = ["du", "-sh", dirpath]
        p = subprocess.run(ducmd, capture_output=True)
        log.info("stdout of %s: %s", ducmd, p.stdout.decode("utf-8").split()[0])
        if p.returncode != 0:
            log.info("stderr of %s: %s", ducmd, p.stderr)
        log.info("removing directory: %s", dirpath)
        shutil.rmtree(dirpath)

    def run(self, source, case=None, **kwargs):
        global OUTPUT_DIR_PREFIX

        if not os.path.exists("/dev/shm"):
            OUTPUT_DIR_PREFIX = os.environ.get("BENCHMARK_OUTPUT_DIR")
            log.info("output dir from env: %s", OUTPUT_DIR_PREFIX)
            if not OUTPUT_DIR_PREFIX:
                sys.exit(
                    "/dev/shm not found (not available on Darwin). Exit. For "
                    "conclusive results, this benchmark should write to tmpfs. "
                    "For local development on e.g. MacOS, you can set the "
                    "environment variable BENCHMARK_OUTPUT_DIR to point to "
                    "a directory to write output to."
                )
            else:
                if not os.path.isdir(OUTPUT_DIR_PREFIX):
                    sys.exit("not a directory: %s", OUTPUT_DIR_PREFIX)

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
                # occupy 10 % or more of this tmpfs). Note that what
                # accumulated in `dirpath` is the result of potentially
                # multiple iterations.
                self._report_dirsize_and_wipe(dirpath)

        # Finally, remove outest directory. Should have no contents by now, but
        # if an individual benchmark iteration was Ctrl+C'd then this here
        # might still do useful cleanup.
        self._report_dirsize_and_wipe(OUTPUT_DIR_PREFIX)

    def _get_benchmark_function(
        self, case, source_name: str, source_ds: ds.Dataset, dirpath: str
    ):
        (selectivity, serialization_format) = case

        # Option A: read-from-disk -> deserialize -> filter -> into memory
        # before timing serialize -> write-to-tmpfs
        t0 = time.monotonic()

        n_rows_only = None
        try:
            n_rows_only = self._get_n_rows[source_name][selectivity]
        except KeyError:
            pass

        if n_rows_only:
            # Pragmatic method for reading only a subset of the data set. A
            # different method for sub selection of rows could use a
            # scanner/filter as in `source_ds.to_table(filter=...)`
            #
            # Note that `head()` returns a `Table` object, i.e. loads data
            # into memory.
            log.info("read %s rows of dataset %s into memory", n_rows_only, source_name)
            data = source_ds.head(
                n_rows_only,
                fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False),
            )
        else:
            log.info("read complete dataset %s into memory", source_name)
            data = source_ds.to_table(
                fragment_scan_options=ds.ParquetFragmentScanOptions(pre_buffer=False)
            )

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
                data=data,
                format=serialization_format,
                base_dir=dirpath,
                # Note(JP): This is important so that if iterations > 1 there
                # is no error sayingthat the target directory is not empty. I
                # have confirmed that this does not affect timing on my machine
                # (as expected, a file system 'overwrite' is as expensive as a
                # new write). Without this technique we can clean up only after
                # n iterations, which consumes n times more space on /dev/shm.
                existing_data_behavior="overwrite_or_ignore",
            )

            # The benchmark function returns `None` for now. If we need
            # deeper inspection into the result maybe iterate on that.

        return benchfunc
