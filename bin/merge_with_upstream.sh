#!/usr/bin/env bash

set -eux -o pipefail

### Fetch upstream and OSS master

git checkout master
git pull upstream master
git remote add oss git@github.com:graphframes/graphframes.git
git fetch oss master

### Create a temp branch from upstream master, and try merging with OSS master

git checkout -b MERGE_WITH_UPSTREAM_TEMP_BRANCH
git merge --commit oss/master

### If that succeeded, then do the same for master, and push changes.
### If it failed, then make fixes, run `git merge --continue`, and
### send a PR based on that branch.  Ignore the rest of the script.

git checkout master
git reset --hard MERGE_WITH_UPSTREAM_TEMP_BRANCH

git push upstream master

### Clean up

git branch -d MERGE_WITH_UPSTREAM_TEMP_BRANCH
git remote remove oss
