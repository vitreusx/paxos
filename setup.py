from setuptools import setup, find_packages
from pathlib import Path


def requires():
    with open(Path(__file__).parent / "requirements.txt") as req_f:
        return req_f.readlines()


setup(
    name="paxos",
    description="Package for DS project on distributed consensus with Paxos.",
    packages=find_packages(),
)
