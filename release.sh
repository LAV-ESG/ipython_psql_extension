#!/usr/bin/bash

# get name of the current release version
LAST_RELEASE=$(git rev-list --tags='v[0-9].[0-9]*' --max-count=1 2>/dev/null)
LAST_TAG=$(git describe --tags $LAST_RELEASE)
CURRENT=$(git rev-parse @ 2>/dev/null)

# check if dirty
MODIFIED=$(git status --porcelain 2>/dev/null | grep | wc -l)
if [ $MODIFIED != 0 ]; then
	echo "ERROR: process aborted"
	echo "There are uncommited changes or untracked files"
	exit 1
fi

echo "latest release: $LAST_RELEASE"
echo "current release: $CURRENT"

# check if we need a new tag
if [ $LAST_RELEASE = $CURRENT ]; then
	echo "ERROR: process aborted"
	echo "This revision is already tagged as release"
	exit 1
fi

# get the new name
NEW_TAG="${LAST_TAG%.*}.$((${LAST_TAG##*.}+1))"
echo "New release is $NEW_TAG"

git tag -a $NEW_TAG
./build.sh