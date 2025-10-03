# Contributing Guide

The GraphFrames project welcomes new contributors. This guide provides an end-to-end
checklist—from preparing your workstation to submitting a pull request—so that you can
iterate with confidence and keep the test suites green.

---

## 1. Prerequisites

Ensure the following tools are installed before cloning the repository:

| Tool | Recommended Version | Notes |
| --- | --- | --- |
| Git | Latest stable | Required for version control and contribution workflows. |
| Java Development Kit (JDK) | 11 or 17 | Spark 3.x supports Java 8/11/17; GraphFrames CI runs on JDK 17. |
| Python | 3.10 – 3.12 | Required for the Python APIs and tests. |
| Apache Spark (binary distribution) | 3.5.x (default) or 4.0.x | Needed for the Python test suite. |
| Poetry | ≥ 1.8 | Dependency manager used by the Python package. Install via [`pipx`](https://pypa.github.io/pipx/) or `pip`. |
| Protocol Buffers compiler (`protoc`) | ≥ 3.21 | Required for the GraphFrames Connect protobuf build. |
| Buf CLI | Latest stable | Used to lint and generate protobuf sources. |
| Apache Spark (optional) | 3.5.x (default) or 4.0.x | Only required if you want the standalone Spark shell outside PySpark. |
| Docker (optional) | Latest stable | Useful for isolated environments but not mandatory. |

### 1.1 Install required tooling

#### macOS (Homebrew)
```bash
brew update
brew install git openjdk@17 python@3.12 pipx protobuf bufbuild/buf/buf
pipx install poetry
```
Add the Java toolchain to your shell profile (for example `~/.zshrc` or `~/.bashrc`):
```bash
export JAVA_HOME="$(/usr/libexec/java_home -v17)"
export PATH="$JAVA_HOME/bin:$PATH"
```

#### Ubuntu / Debian
```bash
sudo apt update
sudo apt install -y git openjdk-17-jdk python3 python3-venv python3-pip curl protobuf-compiler
python3 -m pip install --user pipx
python3 -m pipx ensurepath
pipx install poetry
curl -sSL "https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64.tar.gz" \
  | sudo tar -xzf - -C /usr/local --strip-components=1
```
Add the Java toolchain to your shell profile (for example `~/.zshrc` or `~/.bashrc`):
```bash
export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
export PATH="$JAVA_HOME/bin:$PATH"
```

#### Optional: Standalone Apache Spark distribution
`poetry install` (described later) already brings in the matching version of PySpark and Spark
Connect. If you also want the standalone Spark shell or `spark-submit`, download the distribution
that matches the build’s `spark.version` (currently 3.5.6) and expose it via `SPARK_HOME`:
```bash
curl -O https://downloads.apache.org/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz
mkdir -p "$HOME/.local/spark"
tar -xzf spark-3.5.6-bin-hadoop3.tgz -C "$HOME/.local/spark"
export SPARK_HOME="$HOME/.local/spark/spark-3.5.6-bin-hadoop3"
export PATH="$SPARK_HOME/bin:$PATH"
```

> **Tip:** To build against Spark 4.x, pass `-Dspark.version=<version>` to `./build/sbt` or to the
> jar-building helper script referenced in the Python workflow.

---

## 2. Clone the repository

1. Fork the repository on GitHub (recommended) and copy the clone URL.
2. Clone your fork and switch into the workspace:
   ```bash
   git clone https://github.com/<your-user>/graphframes.git
   cd graphframes
   ```
3. Configure the upstream remote to stay in sync:
   ```bash
   git remote add upstream https://github.com/graphframes/graphframes.git
   git fetch upstream
   ```
4. Create a topic branch for your change:
   ```bash
   git checkout -b feature/my-change
   ```

---

## 3. Scala / JVM workflow

GraphFrames provides a checked-in sbt launcher at `./build/sbt`; a separate sbt
installation is not required.

### 3.1 Compile the project
```bash
./build/sbt compile
```
The first run downloads all dependencies and may take several minutes. If
compilation fails with `-Xfatal-warnings` complaining about deprecated
`java.net.URL` constructors, ensure the build is using your Java 17 toolchain. You
can force it with:
```bash
./build/sbt -java-home "$JAVA_HOME" compile
```

### 3.2 Format Scala code
You can use the project’s [pre-commit hooks](#5-pre-commit-hooks) to automatically 
format all Scala code at once.

### 3.3 Run Scala tests
Run the full test suite:
```bash
./build/sbt test
```
Focus on a specific suite while iterating:
```bash
./build/sbt "core/testOnly org.graphframes.lib.PageRankSuite"
```

### 3.4 Helpful sbt tasks

| Command | Purpose |
| --- | --- |
| `./build/sbt core/assembly` | Builds the uber-jar required for Python tests. |
| `./build/sbt doc` | Generates Scala API documentation. |
| `./build/sbt +package` | Cross-builds artifacts for all supported Scala versions. |
| `./build/sbt scalafmtAll test:scalafmt scalafixAll` | Formats and lints all Scala code using Scalafmt and Scalafix. |
| `./build/sbt docs/laikaPreview` | Serves the documentation site locally at <http://localhost:4242>. |
| `./build/sbt -Dspark.version=4.0.1 compile` | Compiles against Spark 4.x APIs. |
| `./build/sbt package -Dvendor.name=dbx` | Produces Databricks-compatible Spark Connect jars. |

---

## 4. Python workflow

The Python package resides under `python/` and uses Poetry for dependency
management.

To build the GraphFrames assembly jar and run the Python test suite, you can use the provided helper script and `pytest` directly.

1. Install Python dependencies (from the `python/` directory):
   ```bash
   poetry install --with dev,tutorials,docs
   ```

2. Build the required GraphFrames JAR for your Spark version:
   ```bash
   poetry run python ./dev/build_jar.py
   ```
   This script will automatically build the correct JAR for Spark 3.5.x (or 4.x if specified). You do not need to run `sbt` directly.

3. Run the Python test suite:
   ```bash
   poetry run pytest -vvv
   ```
   The test configuration will automatically pick up the correct JAR and Spark version (see [`python/tests/conftest.py`](https://github.com/graphframes/graphframes/blob/main/python/tests/conftest.py) for details).
   
   Enable Spark Connect coverage by exporting `SPARK_CONNECT_MODE_ENABLED=1`:
   ```bash
   SPARK_CONNECT_MODE_ENABLED=1 poetry run pytest -vvv
   ```

4. Optionally enforce formatting and linting:
   ```bash
   poetry run black graphframes tests
   poetry run isort graphframes tests
   poetry run flake8 graphframes tests
   ```

You can see this workflow in action in [the CI configuration](https://github.com/graphframes/graphframes/blob/main/.github/workflows/python-ci.yml).

### 4.1 PySpark smoke tests

After building the assembly jar you can validate your build in an interactive
PySpark session.

1. Export the assembly path from the repository root:
   ```bash
   export GRAPHFRAMES_ASSEMBLY=$(ls ../core/target/scala-${SCALA_VERSION%.*}/graphframes-assembly*.jar | tail -n 1)
   ```
   If the command does not find a jar, rerun `./build/sbt core/assembly` and confirm
   that `SCALA_VERSION` matches the directory under `target/` (for example, `2.12.20`).
2. Launch PySpark from the Poetry environment so the `graphframes` package is on the
   Python path:
   ```bash
   cd python
   poetry run pyspark \
     --driver-memory 2g \
     --jars "$GRAPHFRAMES_ASSEMBLY" \
     --conf spark.driver.extraClassPath="$GRAPHFRAMES_ASSEMBLY" \
     --conf spark.executor.extraClassPath="$GRAPHFRAMES_ASSEMBLY"
   ```
3. In the shell, run a simple PageRank example:
   ```python
   from graphframes import GraphFrame
   from pyspark.sql import SparkSession

   spark = SparkSession.builder.getOrCreate()

   v = spark.createDataFrame([(1,), (2,), (3,)], ["id"])
   e = spark.createDataFrame([(1, 2), (2, 3), (3, 1)], ["src", "dst"])

   g = GraphFrame(v, e)
   g.pageRank(resetProbability=0.15, tol=0.01).vertices.show()
   ```
   Exit the shell with `spark.stop()` or `Ctrl+D` when you are done.
4. Return to the repository root:
   ```bash
   cd ..
   ```

---

## 5. Pre-commit hooks

Enable the bundled `pre-commit` hooks to catch formatting and lint issues before
pushing your branch:
```bash
pipx install pre-commit  # or: pip install pre-commit
pre-commit install
pre-commit run --all-files
```

---

## 6. Making and testing changes

1. Edit the relevant files.
2. Rerun the Scala and/or Python workflows relevant to your changes.
3. Check your working tree:
   ```bash
   git status
   ```
4. Stage and commit with a descriptive message:
   ```bash
   git add <files>
   git commit -m "feat: describe your change"
   ```
5. Push your branch and open a pull request:
   ```bash
   git push origin feature/my-change
   ```
6. Keep your branch current by rebasing on the latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/master
   ```

---

## 7. Quick reference

| Task | Command |
| --- | --- |
| Compile Scala code | `./build/sbt compile` |
| Format Scala code | `./build/sbt scalafmtAll test:scalafmt` |
| Run Scala tests | `./build/sbt test` |
| Run a specific Scala suite | `./build/sbt "core/testOnly <SuiteName>"` |
| Build assembly jar | `./build/sbt core/assembly` |
| Install Python dependencies | `cd python && poetry install --with dev` |
| Run Python tests | `cd python && ./run-tests.sh` |
| Run Python formatters | `poetry run black graphframes tests` |
| Install pre-commit hooks | `pre-commit install` |

You are now ready to iterate on GraphFrames. Refer back to this guide whenever
setting up a new machine or refreshing the local development workflow.