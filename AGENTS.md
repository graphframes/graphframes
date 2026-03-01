# AGENTS.md

This file provides guidance to AI agents when working with code in this repository.

## 🚨 Legacy Codebase Guidelines 🚨

**CRITICAL**: This is a mature, mission-critical codebase (10+ years old in some areas). Follow these rules strictly:

### Regression Prevention
- **NEVER** introduce breaking changes without explicit approval
- **NEVER** modify existing public APIs unless specifically requested
- **NEVER** change existing test assertions or remove tests without approval
- **NEVER** modify CI/build configuration without explicit request
- **ALWAYS** run the complete test suite before committing changes
- **ALWAYS** test against all supported Spark versions

### Code Change Philosophy
- **Prefer addition over modification**: Add new functionality alongside existing code
- **Preserve existing behavior**: If unsure, err on the side of maintaining status quo
- **No "cowboy-style" refactoring**: Avoid large-scale structural changes
- **Conservative by default**: Only make changes that directly address the specific request

### When in Doubt
- **Ask before changing**: If a change seems risky, request explicit approval
- **Start small**: Make minimal changes to achieve the goal
- **Maintain backward compatibility**: Existing user code must continue to work
- **Document reasoning**: Explain why changes are necessary in commit messages

## Build and Development Commands

### Core Build System
- **Build tool**: sbt (Scala Build Tool) version 1.11.0
- **Build project**: `build/sbt compile`
- **Run all tests**: `build/sbt test`
- **Run single test suite**: `build/sbt "testOnly *PregelSuite"`
- **Run single test**: `build/sbt "testOnly *PregelSuite -- -z 'test name pattern'"`
- **Check code formatting**: `build/sbt scalafmtCheckAll`
- **Apply code formatting**: `build/sbt scalafmtAll`
- **Check code style**: `build/sbt "scalafixAll --check"`
- **Generate documentation**: `build/sbt doc`
- **Run with coverage**: `build/sbt "coverage test coverageReport"`

### Multi-Spark Version Testing
The project supports multiple Spark versions. Use `-Dspark.version=X.Y.Z` to specify:
- `build/sbt -Dspark.version=3.5.7 test`
- `build/sbt -Dspark.version=4.0.1 test`
- `build/sbt -Dspark.version=4.1.0 test`

### Pre-PR Checklist Commands
⚠️ **MANDATORY**: These commands MUST pass before any PR. Failures indicate potential regressions.

Before raising a pull request, run these commands to ensure code quality and compatibility:

1. **Format code**: `build/sbt scalafmtAll`
2. **Check formatting**: `build/sbt scalafmtCheckAll` 
3. **Check style/linting**: `build/sbt "scalafixAll --check"`
4. **Build documentation**: `build/sbt doc`
5. **Run tests with coverage**: `build/sbt "coverage test coverageReport"`
6. **Test against multiple Spark versions**:
   - `build/sbt -Dspark.version=3.5.7 test`
   - `build/sbt -Dspark.version=4.0.1 test`
   - `build/sbt -Dspark.version=4.1.0 test`

**Alternative using pre-commit**: `pre-commit run all-files` (handles Python formatting: black, isort, flake8)

### Python Components
- **Python package location**: `/python/` directory
- **Python tests**: Located in `/python/tests/`

## Architecture Overview

### Core Structure
GraphFrames is a graph processing library built on Apache Spark DataFrames with three main components:

1. **Core GraphFrame API** (`core/src/main/scala/org/graphframes/`)
   - `GraphFrame.scala`: Main graph abstraction using DataFrames for vertices/edges
   - Provides graph algorithms, motif finding, and subgraph operations

2. **Algorithm Library** (`core/src/main/scala/org/graphframes/lib/`)
   - Standard graph algorithms: PageRank, ConnectedComponents, ShortestPaths, etc.
   - `Pregel.scala`: Implements Pregel-style bulk synchronous parallel processing
   - `AggregateMessages.scala`: Lower-level message-passing API

3. **GraphX Compatibility Layer** (`graphx/src/main/scala/`)
   - Provides GraphX-compatible APIs for migration
   - Bridges between DataFrame and RDD-based graph operations

### Key Design Patterns

**Spark Version Compatibility**: Uses `SparkShims` pattern for version-specific implementations:
- `core/src/main/scala-spark-3/`: Spark 3.x specific code
- `core/src/main/scala-spark-4/`: Spark 4.x specific code
- Common interface in main scala directory

**DataFrame-Centric Design**: Unlike GraphX's RDD approach, GraphFrames uses DataFrames throughout:
- Vertices and edges are DataFrames with required schema (id column for vertices, src/dst for edges)
- Leverages Spark SQL optimizations and Catalyst query planner
- Allows mixing graph operations with relational queries

**Algorithm Framework**: Most algorithms follow this pattern:
- Extend base traits like `Logging`, `WithLocalCheckpoints`, `WithIntermediateStorageLevel`
- Use builder pattern for configuration (e.g., `graph.pregel.withVertexColumn(...).sendMsgToDst(...).run()`)
- Support checkpointing and persistence for iterative algorithms

### Performance Considerations

**Pregel Optimizations**: The Pregel implementation includes automatic optimizations:
- Detects when destination vertex state is unneeded and skips expensive joins
- Conditional partitioning (source-only vs source+destination)
- Automatic caching of intermediate results when beneficial

**Memory Management**: 
- Uses configurable storage levels for persistence
- Automatic cleanup of intermediate DataFrames between iterations
- Checkpoint support for long-running iterative algorithms

## Testing Structure

### Test Organization
- **Base test class**: `SparkFunSuite` provides SparkContext setup
- **Algorithm tests**: Each algorithm has corresponding `*Suite.scala` in `lib/` subdirectory
- **Integration tests**: `GraphFrameSuite.scala` for core API functionality
- **Pattern matching**: `PatternSuite.scala` for motif finding features

### Running Specific Tests
- Algorithm-specific: `build/sbt "testOnly *PageRankSuite"`
- Core functionality: `build/sbt "testOnly *GraphFrameSuite"`
- Pregel framework: `build/sbt "testOnly *PregelSuite"`
- All lib tests: `build/sbt "testOnly org.graphframes.lib.*"`

### Multi-Version Testing
Tests run against multiple Spark versions in CI. The build system automatically:
- Selects appropriate Scala versions based on Spark version
- Uses version-specific SparkShims implementations
- Validates compatibility across Spark 3.5+ and 4.0+

## Multi-Language Support

### Scala (Primary)
- Main implementation in `core/src/main/scala/`
- Uses Scala 2.12/2.13 depending on Spark version
- Follows Spark's coding conventions and patterns

### Python
- Python bindings in `python/graphframes/`
- Wraps Scala API using Spark's Python gateway
- Separate test suite in `python/tests/`

### Java
- Java examples in `core/src/main/java/`
- Uses Scala API through Java interop
- Follows JavaBean conventions where applicable

## Development Workflow

### Code Quality
- **Formatting**: scalafmt for Scala, black/isort/flake8 for Python
- **Style checking**: scalafix for Scala linting
- **Pre-commit hooks**: Available via `pre-commit run all-files`

### Contribution Requirements
⚠️ **Legacy Codebase Rules**:
- **NO breaking changes** to existing APIs without explicit approval
- **NO modification** of existing test behavior or CI configuration
- **NO large-scale refactoring** or architectural changes
- **ALL changes** must maintain backward compatibility

**Standard Requirements**:
- All new algorithms must include comprehensive test coverage
- Performance-critical code should include benchmarks (`benchmarks/` directory)  
- Multi-version compatibility must be maintained across all supported Spark versions
- Documentation updates required for new features
- **Zero test failures** across all Spark versions before submitting PR

### Release Process
The project uses automated releases through GitHub Actions:
- Scala artifacts published to Maven Central
- Python packages published to PyPI
- Documentation deployed to graphframes.io