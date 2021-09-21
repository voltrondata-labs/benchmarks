import conbench.runner

from benchmarks import _benchmark


def get_valid_cases():
    result = [["query_id", "scale_factor", "format"]]
    for query_id in range(6):
        for scale_factor in [1, 10]:
            for _format in ["native", "parquet", "feather"]:
                result.append([query_id + 1, scale_factor, _format])
    return result


@conbench.runner.register_benchmark
class TpchBenchmark(_benchmark.BenchmarkR):
    external, r_only = True, True
    name, r_name = "tpch", "tpc_h"
    valid_cases = get_valid_cases()

    def run(self, case=None, **kwargs):
        self._set_defaults(kwargs)
        for case in self.get_cases(case, kwargs):
            tags = self.get_tags(kwargs)
            tags["engine"] = "arrow"
            tags["mem_map"] = False
            command = self._get_r_command(kwargs, case)
            yield self.r_benchmark(command, tags, kwargs, case)

    def _set_defaults(self, options):
        options["query_id"] = options.get("query_id", 1)
        options["scale_factor"] = options.get("scale_factor", 1)
        options["format"] = options.get("format", "native")

    def _get_r_command(self, options, case):
        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)}, "
            f"format='{case[2]}', "
            f"scale={case[1]}, "
            f"engine='arrow', "
            f"mem_map=FALSE, "
            f"query_num={case[0]})"
        )
