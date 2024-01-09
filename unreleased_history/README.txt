Adding release notes
--------------------

When adding release notes for the next release, add a file to one of these
directories:

unreleased_history/new_features
unreleased_history/behavior_changes
unreleased_history/public_api_changes
unreleased_history/bug_fixes

with a unique name that makes sense for your change, preferably using the .md
extension for syntax highlighting.

There is a script to help, as in

$ unreleased_history/add.sh unreleased_history/bug_fixes/crash_in_feature.md

or simply

$ unreleased_history/add.sh

will take you through some prompts.

The file should usually contain one line of markdown, and "* " is not
required, as it will automatically be inserted later if not included at the
start of the first line in the file. Extra newlines or missing trailing
newlines will also be corrected.

The only times release notes should be added directly to HISTORY are if
* A release is being amended or corrected after it is already "cut" but not
tagged, which should be rare.
* A single commit contains a noteworthy change and a patch release version bump


Ordering of entries
-------------------

Within each group, entries will be included using ls sort order, so important
entries could start their file name with a small three digit number like
100pretty_important.md.

The ordering of groups such as new_features vs. public_api_changes is
hard-coded in unreleased_history/release.sh


Updating HISTORY.md with release notes
--------------------------------------

The script unreleased_history/release.sh does this. Run the script before
updating version.h to the next development release, so that the script will pick
up the version being released. You might want to start with

$ DRY_RUN=1 unreleased_history/release.sh | less

to check for problems and preview the output. Then run

$ unreleased_history/release.sh

which will git rm some files and modify HISTORY.md. You still need to commit the
changes, or revert with the command reported in the output.


Why not update HISTORY.md directly?
-----------------------------------

First, it was common to hit unnecessary merge conflicts when adding entries to
HISTORY.md, which slowed development. Second, when a PR was opened before a
release cut and landed after the release cut, it was easy to add the HISTORY
entry to the wrong version's history. This new setup completely fixes both of
those issues, with perhaps slightly more initial work to create each entry.
There is also now an extra step in using `git blame` to map a release note
to its source code implementation, but that is a relatively rare operation.
