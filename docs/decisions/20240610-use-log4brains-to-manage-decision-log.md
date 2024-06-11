# Use Log4Brains to manage decision log

- Status: proposed
- Date: 2024-10-06

Technical Story: [ROC-51](https://freelancedevelopercoach.atlassian.net/browse/ROC-51)

## Context and Problem Statement

Decisions related to Rococo are made and kept in Slack. Finding and managing them is difficult (e.g., tracking status, understanding whether they are still actual, obsolete, or superseded).  
  
Instead, we want a solution which will let us:  
- Keep track of the decisions with the long-term consequences in a dedicated place.  
- Capture reasoning and context behind every decision - to be able to get back to it and re-asses in the future. 
- See which decisions are actual, and which ones became obsolete or superceded.  
- Have an easy way to document our experience related to a particular decision at any point in time.  
- Use decision log as and additional documentation for the newcomers.  
- Refer to the decisions easily (e.g. from the documentation or Jira tickets).  
  
## Considered Options
- Slack: [OnlyThreads](https://art-tu19272.slack.com/apps/A022BL4HJLD-onlythreads?tab=more_info) + voting via emojis  
- Slack: [loqbooq](https://art-tu19272.slack.com/apps/A0251FVDR0C-loqbooq?tab=more_info)  
- Slack: [Decision Tracker](https://art-tu19272.slack.com/apps/A010B2YL469-decision-tracker?tab=more_info)  
- GitHub Wiki  
- Markdown files in repositories.  
- [ADR manager](https://github.com/adr/adr-manager)  
- [Log4Brains](https://github.com/thomvaill/log4brains?tab=readme-ov-file#log4brains-)  

## Decision Outcome

Chosen option: ?

## Pros and Cons of the Options

### All Slack tools

Pros:  
- No need to switch to another tool and the entire discussion can be easily captured.  
- Slack provides a number of tools helpful for the decision review process (voting, reminders, etc).
  
Cons:  
- Decisions are harder to manage and harder to link to from the external tools.  
- The decisions are kept separately from other project documentation and code.  
- Scaling to multiple projects will require additional effort (e.g. mapping decisions to projects).

### GitHub Wiki

Pros:  
- Decisions are kept closer to the other documentation and codebase.
- Decisions are scoped per project / repository.
- Easy to link from the external tools.
  
Cons:  
- Lack of support for the workflows specific to decisions. E.g. status change, obsolete / superceded, etc.
- Scaling to multiple projects will require additional effort.

### Markdown files in repositories

Pros:  
- Decisions are kept and versioned along with other project documentation.
- Decisions are scoped per project / repository.
- Easy to link from the external tools.
  
Cons:  
- Lack of support for the workflows specific to decisions. E.g. status change, obsolete / superceded, etc.
- Scaling to multiple projects will require additional effort.

### [ADR manager](https://github.com/adr/adr-manager)

Pros:  
- Includes all benefits of keeping decisions in the markdown files in the repository.
- Provides certain support for the workflows specific to decisions. (creation, search, editing).

Cons:  
- The workflow requires GitHub user with access to the repository.  
- Scaling to multiple projects will require additional effort.  
  
### [Log4Brains](https://github.com/thomvaill/log4brains?tab=readme-ov-file#log4brains-)

Pros:  
- Includes all benefits of keeping decisions in the markdown files in the repository.
- Best support for the workflows specific to decisions (among the reviewed tools).
- Provides a way to support multiple projects, with minimal additional effort.  
  
Cons:  
- Temporarily inactive maintainer. More details [here](https://github.com/thomvaill/log4brains/discussions/108).