from setuptools import setup, find_packages  # type: ignore
import os


def parse_requirements(filename):
    """Load requirements from a pip requirements file."""
    with open(filename, encoding="utf-8") as f:
        # Filter out comments and empty lines.
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


# Read the long description from the README file.
here = os.path.abspath(os.path.dirname(__file__))

# Use requirements.txt to get the list of dependencies.
requirements = parse_requirements(os.path.join(here, "requirements.txt"))

setup(
    name="graphframes",
    version="0.8.4",  # Update this version as needed
    description="GraphFrames: Graph Processing Framework for Apache Spark",
    long_description=open(os.path.join(f"{here}/..", "README.md"), encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    author="GraphFrames Contributors",
    author_email="graphframes@googlegroups.com",
    url="https://pypi.org/project/graphframes-py",
    packages=find_packages(where="python"),
    package_dir={"": "python"},
    include_package_data=True,  # Include non-code files specified in MANIFEST.in
    install_requires=requirements,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
)
