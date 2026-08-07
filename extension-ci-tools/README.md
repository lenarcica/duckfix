# Note you need to clone "extension-ci-tools" to install this package

1. Find at (https://github.com/duckdb/extension-ci-tools)

This is a collection of sample make files and instructions that help build general Duckdb Packages.  However, as duckdb changes versions, and compilers change their default file settings, a certain amount of difficult file copies and transfers need to be made, supporting both Linux ".so" and Windows ".dll" file requirements.

## However

We have previously found that compilation on Windows, depending on Python version, may require edits to the Makefiles supporting extension building.

Certain ```.dll``` or other files could become located in wrong place, and default scripts will make mistakes.  We contain our edited version in folder.  In Linux it is possible that the current locations specified for Debug and Release installs are correct and do not need fixing.

Linux users should be able to use the default extension-ci-tools at this time, but we will still investigate windows builds.
