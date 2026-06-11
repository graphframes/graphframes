#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""Restore mtime of markdown files to their last git commit time."""

import subprocess
from pathlib import Path


def get_git_mtime(filepath: str) -> str | None:
    """Get the last commit time for a file in ISO format."""
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cI", "--", filepath],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or None
    except subprocess.CalledProcessError:
        return None


def set_mtime(filepath: str, timestamp: str) -> bool:
    """Set file mtime using touch -d."""
    try:
        _ = subprocess.run(
            ["touch", "-d", timestamp, filepath],
            check=True,
        )
        return True
    except subprocess.CalledProcessError:
        return False


def main():
    repo_root = Path(__file__).resolve().parent.parent
    docs_dir = (repo_root / "docs" / "src").resolve()

    if not docs_dir.exists():
        print(f"❌ Directory {docs_dir} does not exist")
        return 1

    md_files = list(docs_dir.rglob("*.md"))
    print(f"📁 Found {len(md_files)} markdown files in {docs_dir}")

    success_count = 0
    error_count = 0

    for filepath in sorted(md_files):
        rel_path = filepath.as_posix()

        git_time = get_git_mtime(rel_path)
        if not git_time:
            print(f"⚠️  {rel_path}: no git history found, skipping")
            error_count += 1
            continue

        if set_mtime(rel_path, git_time):
            print(f"✅ {rel_path}: mtime set to {git_time}")
            success_count += 1
        else:
            print(f"❌ {rel_path}: failed to set mtime")
            error_count += 1

    print(f"\n📊 Done: {success_count} updated, {error_count} errors/skipped")
    return 0 if error_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
