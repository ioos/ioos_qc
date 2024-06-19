# Contributors Guide

Interested in helping out the ioos_qc?
Have a few minutes to tackle an issue? Or improve the documentation?
In this guide we will get you setup and integrated into contributing to ioos_qc!

## Introduction

First off, thank you for considering contributing to ioos_qc!

ioos_qc is collection of utilities, scripts and tests to assist in automated quality assurance and quality control for oceanographic datasets and observing systems.
It's people like you that make the ioos_qc useful and successful.
There are many ways to contribute,
from writing tutorials or examples,
improvements to the documentation,
submitting bug reports and feature requests,
or even writing code which can be incorporated into ioos_qc for everyone to use.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing these open source projects.
In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

So, please take a few minutes to read through this guide.

## What Can I Do?

* Tackle any [issues](https://github.com/ioos/ioos_qc/issues) you wish!
* Contribute code you already have. It does not need to be perfect! We will help you clean things up, test it, etc.
* Make a tutorial or example of how to do something.
* Improve documentation of a feature you found troublesome.
* File a new issue if you run into problems!

## Ground Rules

The goal is to maintain a diverse community that's pleasant for everyone. Please be considerate and respectful of others by following our [code of conduct](https://github.com/ioos/.github/blob/main/CODE_OF_CONDUCT.md).

Other items:

* Each pull request should consist of a logical collection of changes. You can include multiple bug fixes in a single pull request, but they should be related.
For unrelated changes, please submit multiple pull requests.
* Do not commit changes to files that are irrelevant to your feature or bug fix
(eg: .gitignore).
* Be willing to accept criticism and work on improving your code; we don't want to break other users' code, so care must be taken not to introduce bugs.
* Be aware that the pull request review process is not immediate, and is generally proportional to the size of the pull request.

## Reporting a bug

The easiest way to get involved is to report issues you encounter when using ioos_qc or by requesting something you think is missing.

* Head over to the project [issues](https://github.com/ioos/ioos_qc/issues) page.
* Search to see if your issue already exists or has even been solved previously.
* If you indeed have a new issue or request, click the "New Issue" button.
* Fill in as much of the issue template as is relevant. Please be as specific as possible. Include the version of the code you were using, as well as what operating system you are running. If possible, include complete, minimal example code that reproduces the problem.

## Setting up your development environment

We recommend using the [conda](https://conda.io/docs/) package manager for your Python environments.

Install [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
(link with instructions) on your system if not already available
(check with ``git --version`` at the command line.)
This can also be installed from a variety of package managers, including ``conda`` if needed.

Login to your [GitHub](https://github.com) account and make a fork of the
[ioos_qc repository](https://github.com/ioos/ioos_qc) by clicking the "Fork" button.
Clone your fork of the repository (in terminal on Mac/Linux or git shell/GUI on Windows)
to the location you'd like to keep it.
We are partial to creating a ``git_repos`` or ``projects`` directory in our home folder.

```sh
git clone https://github.com/<your-user-name>/ioos_qc.git
```

Navigate to that folder in the terminal or in Anaconda Prompt if you're on Windows.
The remainder of the instructions will take place within this directory.

```sh
cd ioos_qc
```

Connect your repository to the upstream (main project).

```sh
git remote add upstream https://github.com/ioos/ioos_qc.git
```

Create a new conda environment for us to configure, and give it a name.

For Environment Setup:

After ``-n`` you can specify any name you'd like; here we've chosen ``ioosqc312``.

```sh
conda create -n ioosqc312 python=3.12
```

**IMPORTANT**: Always activate this environment when developing and testing your changes!

```sh
conda activate ioosqc312
```

You will have to do this any time you re-open your prompt.
Currently there are no packages in this environment, let's change that.

```sh
conda install --file requirements.txt --file requirements-dev.txt
```

Note that those requirement file names may change depending on the repository.
Again, always check the local README for more information.

Finally, to run all the tests!

```sh
python -m pytest --disable-warnings
```

Once you are done with environemtn setup, its time to build docs:

Create the following environment:

```sh
conda create -y -n ioosqc312_docs python=3.12
```

**IMPORTANT**: Always activate this environment when developing and testing your changes!

```sh
conda activate ioosqc312_docs
```

You will have to do this any time you re-open your prompt.
Currently there are no packages in this environment, let's change that.

```sh
conda install -y --file requirements.txt --file requirements-dev.txt --file docs/requirements.txt
```

Note that those requirement file names may change depending on the repository.
Again, always check the local README for more information.

```sh
cd docs
make html
```
Then open a browser to:
``build/html/index.html``

**Note:** If you encounter problems during the build, delete the ``docs/source/examples/.ipynb_checkpoints/`` folder and try again.

Run Jupyter server and edit notebook examples:
```sh
conda activate ioosqc312_docs
cd docs/source/examples
jupyter notebook
```

**Note:** If you add or edit the notebook examples, you should save the result with the *output cleared on all cells*.
This way, ``nbsphinx`` will build the notebook output during the build stage.

Now you're all set!
You have an environment called ``ioosqc312`` that you can work in.
Remember, you will need to activate this environment the
next time you want to use it after closing the terminal.
If you want to get back to the root environment, run ``conda deactivate``.

## Pull Requests

The changes to the ioos_qc source (and documentation)
should be made via GitHub pull requests against ``main``,
even for those with administration rights.
While it's tempting to make changes directly to ``main`` and push them up,
it is better to make a pull request so that others can give feedback.
If nothing else,
this gives a chance for the automated tests to run on the PR.
This can eliminate "brown paper bag" moments with buggy commits on the main branch.


During the Pull Request process, before the final merge, it's a good idea to rebase the branch
and squash together smaller commits. It's not necessary to flatten the entire branch, but it
can be nice to eliminate small fixes and get the merge down to logically arranged commits. This
can also be used to hide sins from history--this is the only chance, since once it hits
``main``, it's there forever!

**Working on your first Pull Request?** You can learn how from this *free* video series
[How to Contribute to an Open Source Project on GitHub](https://egghead.io/courses/how-to-contribute-to-an-open-source-project-on-github),
Aaron Meurer's [tutorial on the git workflow](https://www.asmeurer.com/git-workflow/), or the
guide [“How to Contribute to Open Source"](https://opensource.guide/how-to-contribute/).

Commit the changes you made. Chris Beams has written a [guide](https://cbea.ms/git-commit/)
on how to write good commit messages.

Push to your fork and submit a pull request.


## What happens after the pull request

You've made your changes, documented them, added some tests, and submitted a pull request.
What now?

### Automated Testing

First, our army of never sleeping robots will begin a series of automated checks.
The test suite,
documentation,
style,
and more will be checked on various versions of Python with current and legacy packages.
CIs will run testing on Linux, and Mac, and Windows.
Other services will kick in and check if there is a drop in code coverage
or any style variations that should be corrected.
If you see a red mark by a service,
something failed and clicking the "Details" link will give you more information.
We're happy to help if you are stuck.

The robots can be difficult to satisfy, but they are there to help everyone write better code.
In some cases, there will be exceptions to their suggestions, but these are rare. If you make
changes to your code and push again, the tests will automatically run again.

### Code Review

At this point you're waiting on us. You should expect to hear at least a comment within a
couple of days. We may suggest some changes or improvements or alternatives.

Some things that will increase the chance that your pull request is accepted quickly:

* Write tests.
* Fix any failed lints shown by pre-commit-ci.
* Write a [good commit message](https://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

Pull requests will automatically have tests run by CIs.
This includes running both the unit tests as well as the code linters.

### Merging

Once we're all happy with the pull request, it's time for it to get merged in. Only the
maintainers can merge pull requests and you should never merge a pull request you have commits
on as it circumvents the code review. If this is your first or second pull request, we'll
likely help by rebasing and cleaning up the commit history for you. As your development skills
increase, we'll help you learn how to do this.

## Further Reading

There are a ton of great resources out there on contributing to open source and on the
importance of writing tested and maintainable software.

* [How to Contribute to Open Source Guide](https://opensource.guide/how-to-contribute/)
* [Zen of Scientific Software Maintenance](https://jrleeman.github.io/ScientificSoftwareMaintenance/)
