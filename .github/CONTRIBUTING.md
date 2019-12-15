# Contributing guide

Thanks for considering contributing to ZIO-keeper!

Please take a moment to review this document in order to make the contribution process easy and
effective for everyone involved.

## Bug reports

Well-written, thorough bug reports are a great way to contribute to the project.

Before raising a new issue, please check [our issues list][link-issues] to determine whether the
issue you encountered has already been reported.

Note that a good bug report should be self-explanatory, therefore it is very important to be as
detailed as possible. The following questions might serve as a template for writing such reports:

* What were you trying to achieve?
* What are the expected results?
* What are the received results?
* What are the steps to reproduce the issue?
* In what environment did you encounter the issue?

## Feature requests

Feature requests are welcome. Please take your time to document the feature as much as possible - it
is up to you to convince the project's maintainers of the merits of this feature, and its alignment
with the scope and goals of the project.

## Pull requests

Good pull requests (e.g. patches, improvements, new features) are a fantastic help. They should
remain focused in scope and avoid containing unrelated commits.

Please ask first before embarking on any significant pull request (e.g. implementing features or
refactoring code), otherwise you risk spending a lot of time working on something that the project's
maintainers might not want to merge into the project.

To start contributing, fork the project, clone your fork, and configure the remotes:

```bash
git clone https://github.com/<your-username>/zio-keeper.git
cd zio-keeper
git remote add upstream https://github.com/zio/zio-keeper.git
```

If you cloned a while ago, make sure to update your branch with the changes from upstream:

```bash
git pull --rebase upstream master
```

Create a new topic branch off the **master** and push it to your fork:

```bash
git checkout -b <branch-name>
git push origin <branch-name>
```

Before submitting a pull request, make sure the following requirements are met:

* Changes are committed in small, coherent and compilable increments.
* Every commit has a [Good Commit Message™][link-otp].
* Existing tests don't fail.

Once ready, [open a pull request][link-pr] with a clear title and description.

[link-issues]: https://github.com/zio/zio-keeper/issues
[link-otp]: https://github.com/erlang/otp/wiki/Writing-good-commit-messages
[link-pr]: https://help.github.com/articles/about-pull-requests/
