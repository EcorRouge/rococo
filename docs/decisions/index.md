<!-- This file is the homepage of your Log4brains knowledge base. You are free to edit it as you want -->

# Decision log
[![Rococo Decisions](https://ecorrouge.github.io/rococo/badge.svg)](https://ecorrouge.github.io/rococo)  
Welcome 👋 to the decision log of the Rococo platform.

## Definition and purpose

> An Architectural Decision (AD) is a software design choice that addresses a functional or non-functional requirement that is architecturally significant.
> An Architectural Decision Record (ADR) captures a single AD, such as often done when writing personal notes or meeting minutes; the collection of ADRs created and maintained in a project constitutes its decision log.

Maintaining this documentation aims at:

- Keep track of the decisions with the long-term consequences in a dedicated place.  
- Capture reasoning and context behind every decision - to be able to get back to it and re-asses in the future. 
- See which decisions are actual, and which ones became obsolete or superceded.  
- Have an easy way to document our experience related to a particular decision at any point in time.  
- Use decision log as and additional documentation for the newcomers.  
- Refer to the decisions easily (e.g. from the documentation or Jira tickets).

## Usage

The developers manage this documentation directly with markdown files located next to their code, so it is more convenient for them to keep it up-to-date.

This website is automatically updated after a change on the `main` branch of the project's Git repository (via the `publish-log4brains.yml` GitHub Workflow).

You can browse the ADRs by using the left menu or the search bar.

The typical workflow of a decision is the following:
![Decision workflow](/l4b-static/adr-workflow.png)

The decision process is entirely collaborative and backed by pull requests.

## More information

- [Log4brains documentation](https://github.com/thomvaill/log4brains/tree/master#readme)
- [What is an ADR and why should you use them](https://github.com/thomvaill/log4brains/tree/master#-what-is-an-adr-and-why-should-you-use-them)
- [ADR GitHub organization](https://adr.github.io/)