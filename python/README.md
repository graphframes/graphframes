# GraphFrames `graphframes-py` Python Package

The is the officila [graphframes-py PyPI package](https://pypi.org/project/graphframes-py/), which is a Python wrapper for the Scala GraphFrames library. This package is maintained by the GraphFrames project and is available on PyPI.

For instructions on GraphFrames, check the project [../README.md](../README.md). See [Installation and Quick-Start](#installation-and-quick-start) for the best way to install and use GraphFrames.

## Running `graphframes-py`

You should use GraphFrames via the `--packages` argument to `pyspark` or `spark-submit`, but this package is helpful in development environments.

```bash
# Interactive Python
$ pyspark --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12

# Submit a script in Scala/Java/Python
$ spark-submit --packages graphframes:graphframes:0.8.4-spark3.5-s_2.12 script.py
```
