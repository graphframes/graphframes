#!/usr/bin/env bash

set -eux -o pipefail

OSS_URL="git@github.com:graphframes/graphframes.git"
DB_URL="git@github.com:databricks/graphframes.git"
BRANCH="master"

OSS_REMOTE="oss"
DB_REMOTE="databricks"

setRemote () {
  if git remote get-url $1 ; then
    git remote set-url $1 $2
  else
    git remote add $1 $2
  fi
}

setRemote $OSS_REMOTE $OSS_URL
setRemote $DB_REMOTE $DB_URL
git remote set-url --push $OSS_REMOTE DISABLE 

git stash

git fetch $DB_REMOTE $BRANCH
git checkout $DB_REMOTE/$BRANCH
MERGE_BRANCH="zzz_MERGE_$(date +'%Y-%m-%d-%H-%M-%S')"
git checkout -b $MERGE_BRANCH

echo "Merging ... if failed, please resolve conflicts and make a PR."
git pull $OSS_REMOTE $BRANCH

git push $DB_REMOTE $MERGE_BRANCH:$BRANCH
# keep the branch for future reference
