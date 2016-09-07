#!/usr/bin/env python
# coding=utf-8

from setuptools import setup
from setuptools import find_packages


setup(
    name="graphframes",
    version="0.2.1",
    description="GraphFrames: DataFrame-based Graphs",
    url="https://github.com/graphframes/graphframes",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[],
    license="ASF 2.0",
    zip_safe=False,
    keywords=["graphframes", "spark", "graph", "dataframe"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
    ],
)
