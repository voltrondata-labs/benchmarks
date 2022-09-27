import pathlib
from typing import List

import setuptools


def read_requirements_file(filepath: pathlib.Path) -> List[str]:
    """Parse a requirements.txt file into a list of package requirements"""
    with open(filepath, "r") as f:
        requirements = [
            line.strip() for line in f if line.strip() and not line.startswith("#")
        ]
    return requirements


repo_root = pathlib.Path(__file__).parent

with open(repo_root / "README.md", "r") as f:
    long_description = f.read()

base_requirements = read_requirements_file(repo_root / "requirements.txt")
dev_requirements = read_requirements_file(repo_root / "requirements-dev.txt")


setuptools.setup(
    name="benchmarks",
    version="0.0.1",
    description="Apache Arrow Benchmarks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    entry_points={},
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.8",
    maintainer="Apache Arrow Developers",
    maintainer_email="dev@arrow.apache.org",
    url="https://github.com/voltrondata-labs/benchmarks",
    install_requires=base_requirements,
    extras_require={"dev": dev_requirements},
)
