import uuid

import conbench.runner

from benchmarks import _benchmark


def get_valid_cases():
    result = [["query_id", "scale_factor", "format"]]
    for query_id in range(1, 11):
        for scale_factor in [1, 10]:
            for _format in ["native", "parquet", "feather"]:
                result.append([query_id, scale_factor, _format])
    return result


@conbench.runner.register_benchmark
class TpchBenchmark(_benchmark.BenchmarkR):
    external, r_only = True, True
    name, r_name = "tpch", "tpc_h"
    valid_cases = get_valid_cases()

    def run(self, case=None, **kwargs):
        self._set_defaults(kwargs)
        run_id = kwargs.get("run_id")
        run_id = run_id if run_id else uuid.uuid4().hex
        for case in self.get_cases(case, kwargs):
            tags = self.get_tags(kwargs)
            tags["engine"] = "arrow"
            tags["memory_map"] = False
            tags["query_id"] = f"TPCH-{case[0]:02d}"
            kwargs["run_id"], kwargs["batch_id"] = self._get_ids(run_id, case)
            command = self._get_r_command(kwargs, case)
            yield self.r_benchmark(command, tags, kwargs, case)

    def _set_defaults(self, options):
        options["query_id"] = int(options.get("query_id", 1))
        options["scale_factor"] = int(options.get("scale_factor", 1))
        options["format"] = options.get("format", "native")

    def _get_ids(self, run_id, case):
        # manually batch so that the batch plots display
        (_, scale_factor, _format) = case
        return run_id, f"{run_id}-{scale_factor}{_format[0]}"

    def _get_r_command(self, options, case):
        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)}, "
            f"format='{case[2]}', "
            f"scale_factor={case[1]}, "
            f"engine='arrow', "
            f"memory_map=FALSE, "
            f"query_id={case[0]})"
        )
