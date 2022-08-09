import setuptools

with open("README.md", "r") as readme:
    long_description = readme.read()


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
)
