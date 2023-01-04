from pathlib import Path

from setuptools import find_packages, setup


def requires():
    with open(Path(__file__).parent / "requirements.txt") as req_f:
        return req_f.readlines()


setup(
    name="paxos",
    version="0.0.1-dev",
    description="Package for DS project on distributed consensus with Paxos.",
    packages=find_packages(),
    install_requires=requires(),
    extras_require={
        "dev": [
            "black>=22.12.0",
            "isort>=5.11.3",
            "mypy>=0.991",
        ]
    },
)
