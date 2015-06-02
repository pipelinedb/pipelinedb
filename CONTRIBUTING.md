Contributing
============

We're stoked you want to contribute to PipelineDB! You can help us in a few different ways:

- [Create an issue](https://github.com/pipelinedb/pipelinedb/issues/new) to report a bug or suggest an improvement.
- Improve the [documentation](https://github.com/pipelinedb/docs).
- Submit a code patch for an existing issue.

Submitting a Patch
------------------
The rough flow for submitting a patch is:

- Sign our [Contributor License Agreement](http://pipelinedb.com/cla), if you haven't done so already.
- Create a fork of the [PipelineDB repository](https://github.com/pipelinedb/pipelinedb).
- Make code changes and push them to your fork. Make sure to include tests for your changes.
- Submit a pull request against our `master` branch. Please rebase and squash all your commits before submitting the pull request. edx has a great [write-up](https://github.com/edx/edx-platform/wiki/How-to-Rebase-a-Pull-Request) on how to rebase a pull request.
- Your pull request must receive a lgtm from a core committer before it's ready to be merged.

Coding Style
------------
We try to follow PostgreSQL's [coding style guide](http://www.postgresql.org/docs/devel/static/source.html)—we're not too pedantic about it though. Just make sure your code is clean and well commented.

We've shamelessly copied etcd's [commit message format](https://github.com/coreos/etcd/blob/master/CONTRIBUTING.md#format-of-the-commit-message):

```
<subsystem>: <what changed>
<BLANK LINE>
<why this change was made>
<BLANK LINE>
<issue number, if applicable>
```

Wrap all lines in the commit message around 80 characters.
