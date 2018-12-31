# kinesis-data-analytics-ansible
Ansible modules for managing AWS Kinesis Data Analytics resources

# Library Notes

## Non-idiomatic Documentation/Argument Specs

As the API covered by this library is, in some cases, very complex, the
modules in this library use nested dictionaries and lists of dictionaries
to better organize arguments.  This requires a non-standard approach to
produce usable documentation, which is included as WEBDOCS.md in the repo.

## API Coverage

Currently, the following resources are completely or partially covered:

- Kinesis Data Analytics

## Gaps

- Pagination is not covered for any API.
- Updates/Patches represent a subset of all possible operations.  While
  coverage is generally robust, there are a few exceptions that are not
  covered, and those should be noted in the docs and modules.

## Other Notes

- Issues and PRs are welcome!  Tests are expected with any code changes.

# DANGER WILL ROBINSON!!!

This module is not for you if:
- You use Flink as runtime, this module only support SQL as runtime for Kinesis Data Analytics application.
