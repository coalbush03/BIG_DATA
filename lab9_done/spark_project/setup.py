from setuptools import setup, find_packages

setup(
    name="pyspark_demo",
    version="0.1",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=["pyspark"],
    entry_points={
        "console_scripts": [
            "pyspark-demo = main:main",
        ],
    },
)
