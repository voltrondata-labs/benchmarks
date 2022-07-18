import os

import pytest

pytest.register_assert_rewrite("benchmarks.tests._asserts")

# set to not try to send anything to conbench
os.environ["DRY_RUN"] = "1"


def pytest_addoption(parser):
    parser.addoption(
        "--run-slow",
        action="store_true",
        default=False,
        help="run slow tests",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow to run",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-slow"):
        return
    reason = "need --run-slow option to run"
    skip_slow = pytest.mark.skip(reason=reason)
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)
