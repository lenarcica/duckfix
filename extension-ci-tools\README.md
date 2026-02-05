# Note you need to clone "extension-ci-tools" to install this package

1. Find at (https://github.com/duckdb/extension-ci-tools)

## However

We have found that compilation on Windows, depending on Python version, will require edits to the Makefiles supporting extension building.

Certain ```.dll``` or other files are located in wrong place, and default scripts will make mistakes.  We contain our edited version in folder.
