from setuptools import find_packages, setup

if __name__ == "__main__":
    setup(
        name="hooli_ml_pipelines",
        version="1.3.5",
        author="Hooli",
        author_email="hello@hooli.com",
        description="Hooli ML team Dagster pipelines.",
        url="https://github.com/dagster-io/hooli-ml-pipelines",
        classifiers=[
            "Programming Language :: Python :: 3.10",
        ],
        packages=find_packages(exclude=["hooli_ml_pipelines_tests"]),
        install_requires=[
            "boto3",
            "dagit",
            "dagster",
            "dagster_aws",
            "dagster_cloud",
            "dagstermill",
            "matplotlib",
            "pandas<1.4.0",
            "pyspark",
            "sklearn",
            "snowflake-sqlalchemy",
        ],
        extras_require={
            "test": ["black", "mypy", "pylint", "pytest"],
        },
    )
