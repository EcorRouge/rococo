# Decision Log Overview
[Visit Rococo Decision Log](https://ecorrouge.github.io/rococo),  
here is how many decisions (ADRs) it currently contains: [![Rococo Decisions](https://ecorrouge.github.io/rococo/badge.svg)](https://ecorrouge.github.io/rococo)  

## How to add new decision

### 1. Create new decision markdown file on your local PC
There are two ways to create a new decision - manual and partially automated via CLI.

#### Manual way: by copying the decision template:
- Make a copy of the template at [docs\decisions\template.md](https://github.com/EcorRouge/rococo/blob/main/docs/decisions/template.md).
- Rename the copy following this format: `YYYYMMDD-title-which-makes-sense.md`  
  Ideally, the title specified within the file (top row) has to match the title specified in the filename (except for the formatting).

#### Partially automated way: via Log4Brains CLI:
see [how to install and use the CLI](https://github.com/thomvaill/log4brains/tree/master?tab=readme-ov-file#-getting-started) and the [required prerequisites](https://github.com/thomvaill/log4brains/tree/master?tab=readme-ov-file#what-are-the-prerequisites).

### 2. Edit the markdown file
- Use your favorite editor.
- Most of the sections are optional, feel free to omit.  
  The suggested minimum to fill in is: `title` (top row), `Status`, `Date` (YYYY-MM-DD format), `Context and Problem Statement`, `Decision Outcome`.  All fields are described in the template itself.  
- A couple of samples: [Rococo's standard ID](https://ecorrouge.github.io/rococo/adr/20240611-rococo-standard-id/), [Use Log4Brains to manage the decision log](https://ecorrouge.github.io/rococo/adr/20240610-use-log4brains-to-manage-decision-log/) . *Follow the GitHub link in the top right corner to open the raw .md files on GitHub*

### 3. Push the changes to the main branch
- Create a PR containing changes with the new decision file.  
  Sometimes new decision will accompany code changes.
- Merge the PR into the `main` branch.
- Within a couple of minutes you should find new decision published at https://ecorrouge.github.io/rococo/


## Decision lifecycle and statuses
![](https://ecorrouge.github.io/rococo/l4b-static/adr-workflow.png)

`DRAFT` - the decision is yet to be discussed
`PROPOSED` - the decision is being discussed (e.g. draft was created and the creator published link to the draft in Slack or started the conversation in the PR with the new decision)

`REJECTED`, `ACCEPTED`, `DEPRECATED`, `SUPERSEDED` - self-explanatory.

## How to change decision status
All statuses are set by editing the `Status` row of the decision / decision template.

In case of `SUPERSEDED` status the decision record `Status` row should include the link to the superseding decision. See the [example of the superseded decision in the Log4Brains decision log](https://raw.githubusercontent.com/thomvaill/log4brains/master/docs/adr/20200926-use-the-adr-number-as-its-unique-id.md) and [how it looks like in the UI](https://thomvaill.github.io/log4brains/adr/adr/20200926-use-the-adr-number-as-its-unique-id/).

 Log4brains CLI provides an option to supersede a decision record during the creation of the new record.  

## Implementation details
Each decision is represented by a markdown file in the [docs\decisions](https://github.com/EcorRouge/rococo/tree/main/docs/decisions). Every commit to the `main` branch with the changes under the `docs\decisions` directory path triggers [GitHub action](https://github.com/EcorRouge/rococo/blob/main/.github/workflows/publish-log4brains.yml), which creates a static website, stores it in the [gh-pages branch](https://github.com/EcorRouge/rococo/tree/gh-pages), and publishes it to the GitHub Pages at [https://ecorrouge.github.io/rococo/](https://ecorrouge.github.io/rococo/). The website visualizes the decision log and provides search capabilities.

## Future plans
Rococo consists of several repositories. We are considering a dedicated repository for all Rococo documentation. Commits to the docs in the Rococo repositories could be automatically synced to the dedicated repository, which would have all the tooling and automation necessary to build and publish the documentation.