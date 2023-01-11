# Sightmachine fork

## Git Setup

Keep the remote names as `sightmachine` and `facebook`

```
> git remote -v
facebook        git@github.com:facebook/rocksdb (fetch)
facebook        git@github.com:facebook/rocksdb (push)
sightmachine    git@github.com:sightmachine/rocksdb (fetch)
sightmachine    git@github.com:sightmachine/rocksdb (push)

```

## Git flow

facebook:
* work happens in multiple branches
* branches are named `7.8.fb`
* tags are named `v7.8.3`

sightmachine:
* work happen in only one _current_ branch, and it's NOT named `main` or `master`
* tag `v7.8.3` will form base of branch `sm-main-7.8.3`
* *this* will be git default branch (https://github.com/sightmachine/rocksdb/settings/branches)
* sightmachine patches will be added on top of it.

* When switching to another upstream branch ( e.g. 7.9.2)
  * push facebook tag to sightmachine `git push sightmachine v7.9.2`
  * branch `sm-main-7.9.2` create from upstream tag `v7.9.2`
  * the changes from `v7.8.3...sm-main-7.8.3` cherry-picked to `sm-main-7.9.2`
	* squashing the change makes may make it easier to apply this step.
	* merge-commits can wreck this step, avoid them.
	* run `git diff v7.7.3...v7.8.3 -- Makefile java/Makefile` to see if new source files (e.g. tests) need to be added to CMakeLists.txt.
  * git default branch change to `sm-main-7.9.2` https://github.com/sightmachine/rocksdb/settings/branches


* a release will be marked by tag named `sm-v7.8.3.1`, `.2` etc.
  * note that we do NOT update `include/rocksdb/version.h`. The build-suffix(`.2`) is ONLY present in tag name.
* For SM specific changes
  * A PR based on branch `sm-main-7.8.3` will be opened
  * The pr will be merge-ff to `sm-main-7.8.3`
	* don't create merge-commits as it'll be problem when cherry-picking to next version.
  * tag `sm-v7.8.3.4` will be placed to make a release build.


## CirclCI setup

We want to actively mirror few fb branches to our fork,
but do not want to trigger circlci on these,
so we use "Only build pull requests: ON"
https://app.circleci.com/settings/project/github/sightmachine/rocksdb/advanced
Note that the default branch will always be build by CirclCI after merge.

* build on any branch at tag named `sm-v7.8.3.1` makes RELEASE builds
  * version in jfrog would be `7.8.3.1`
* anything else makes SNAPSHOT builds, with version from `includes/rocksdb/version.h`
  * version in jfrog would be `7.8.3-SNAPSHOT`

* Pushing a tag makes a release build.
```
git switch sm-main-7.8.3
git tag sm-v7.8.3.4
git push sightmachine sm-v7.8.3.4
```

The version `sm-rocksdb:sm-rocksdbjni:7.8.3.4` will be available after half an hour.

## Sync branches

Syncing up branches to our repo allows us to view our patch better in the
github's PR view.

We only want to sync selected branches
* 7.7.fb
* 7.8.fb
* 7.9.fb
* main

This can be done by github's UI ( preferred) , or from console also:

```
## get everything, get rid of gone branches.
git fetch --prune --all

## make sure nothing is checked out, as we'll switch branches.
git status --short --branch

syncbranch()
{
	   git switch sm-head &&
	   git switch --track facebook/$1 &&
	   git rebase facebook/$1 &&
	   git push sightmachine &&
	   git switch sm-head
}

syncbranch 7.7.fb
syncbranch 7.8.fb
syncbranch 7.9.fb

```

Do NOT needlessly migrate branch and tag from facebook to sightmachine repo, they only
clutter without adding any value.
