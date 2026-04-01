#!/usr/bin/env python3

import argparse
import json
import subprocess
from pathlib import Path


def run_sbt(sbt_cmd: str, spark_version: str, jmh_args: str, repo_root: Path) -> None:
    subprocess.run(
        [sbt_cmd, f"-Dspark.version={spark_version}", jmh_args],
        check=True,
        cwd=repo_root,
    )


def load_json_array(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array in {path}")
    return data


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Run benchmarks needed for docs generation")
    parser.add_argument("--spark-version", default="4.0.0")
    parser.add_argument("--sbt", default=str((repo_root / "build" / "sbt").resolve()))
    args = parser.parse_args()

    results_dir = (repo_root / "benchmarks" / "target" / "doc-jmh").resolve()
    results_dir.mkdir(parents=True, exist_ok=True)

    benchmark_commands = [
        (
            results_dir / "shortest-paths.json",
            "benchmarks/Jmh/run -rf json -p graphName=wiki-Talk -p useLocalCheckpoints=true "
            "-p algorithm=graphframes,graphx "
            "org.graphframes.benchmarks.ShortestPathsBenchmark",
        ),
        (
            results_dir / "connected-components.json",
            "benchmarks/Jmh/run -rf json -p graphName=wiki-Talk -p useLocalCheckpoints=true "
            "-p algorithm=graphframes,graphx -p broadcastThreshold=-1 "
            "org.graphframes.benchmarks.ConnectedComponentsBenchmark",
        ),
        (
            results_dir / "label-propagation.json",
            "benchmarks/Jmh/run -rf json -p graphName=wiki-Talk -p useLocalCheckpoints=true "
            "-p algorithm=graphframes,graphx "
            "org.graphframes.benchmarks.LabelPropagationBenchmark",
        ),
    ]

    all_results: list[dict] = []
    for output_file, cmd in benchmark_commands:
        run_sbt(args.sbt, args.spark_version, f"{cmd} -rff {output_file}", repo_root)
        all_results.extend(load_json_array(output_file))

    target_file = (repo_root / "benchmarks" / "jmh-result.json").resolve()
    with target_file.open("w", encoding="utf-8") as file:
        json.dump(all_results, file, indent=2)
        file.write("\n")

    print(f"Saved {len(all_results)} benchmark entries to {target_file}")


if __name__ == "__main__":
    main()
