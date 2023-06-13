import pytest

from .._benchmark import BenchmarkR


class TestBenchmarkR:
    @pytest.mark.parametrize("error_type", ["NULL", "'base'"])
    def test_r_benchmark(self, error_type: str):
        class PlaceboBenchmark(BenchmarkR):
            name, r_name = "placebo", "placebo"

            def run():
                pass

        benchmark = PlaceboBenchmark()

        r_command = (
            f"library(arrowbench); "
            f"run_one(arrowbench:::placebo, error_type={error_type})"
        )

        result, output = benchmark.r_benchmark(
            command=r_command, extra_tags={}, options={}
        )

        if error_type == "NULL":
            assert float(result["stats"]["data"][0]) > 0.0
            assert result.get("error") is None
        elif error_type == "'base'":
            assert "stats" not in result
            assert (
                "Error in placebo_func(): something went wrong (but I knew that)"
                in result["error"]["error"]
            )
