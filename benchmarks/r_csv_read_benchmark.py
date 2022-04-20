import itertools

import conbench.runner

from benchmarks import _benchmark


@conbench.runner.register_benchmark
class RCsvReadBenchmark(_benchmark.BenchmarkR):
    external, r_only = True, True
    name, r_name = "r-csv-read", "read_csv"
    valid_cases = [
        ('source', 'compression', 'output'),
        *itertools.product(
            ['fanniemae_2016Q4', 'nyctaxi_2010-01', 'chi_traffic_2020_Q1'],
            ['uncompressed', 'gzip'],
            ['arrow_table', 'data_frame']
        )
    ]

    def run(self, case=None, **kwargs):
        self._set_defaults(kwargs)

        for case in self.get_cases(case, kwargs):
            tags = self.get_tags(kwargs)
            tags['source'] = case[0]
            tags["reader"] = 'arrow'
            tags["compression"] = case[1]
            tags["output"] = case[2]
            self._manually_batch(kwargs, case)
            command = self._get_r_command(kwargs, case)
            yield self.r_benchmark(command, tags, kwargs, case)

    def _set_defaults(self, options):
        options["source"] = options.get("source", 'fanniemae_2016Q4')
        options["compression"] = options.get("compression", 'uncompressed')
        options["output"] = options.get("output", "arrow_table")

    def _manually_batch(self, options, case):
        # manually batch so that the batch plots display
        (source, compression, output) = case
        run_id = self.conbench.get_run_id(options)
        batch_id = f"{run_id}-{source}-{compression}-{output}"
        self.conbench.manually_batch(batch_id)

    def _get_r_command(self, options, case):
        params = {
            parameter: argument
            for parameter, argument in zip(self.valid_cases[0], case)
        }

        return (
            f"library(arrowbench); "
            f"run_one({self.r_name}, "
            f"cpu_count={self.r_cpu_count(options)}, "
            f"source=\"{params['source']}\", "
            f"reader='arrow', "
            f"compression=\"{params['compression']}\", "
            f"output=\"{params['output']}\")"
        )
