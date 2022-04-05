# 2. Store Per Environment Configuration Files in Ceres Repo

Date: 2022-04-04

## Status

Accepted

triggers [3. Ensure Unit tests for Configuration Files](0003-ensure-unit-tests-for-configuration-files.md)

## Context

Ceres will be deployed to multiple environments. Each env requires its own configuration file to manage.
We could store these configurations in separate repo or ceres repo, or repo with helm charts etc.


## Decision

- Keep the files in ceres repo so that they are convenient to devs for easy changes + PR.
- Prefer yaml content in config map file instead of string value containing yaml
    ```
   * DO NOT *
    application.yml: |-
      ceres:
        downsample:
          
  
    * DO * 
    application.yml:
      ceres:
        downsample:      
    ```

## Consequences

- Easier for devs to change
- Controlled through normal PRs process
- Storing the config in repo hinders the ability to be a public repo because it expose sensitive values
- Helps devs to visualize property values in staging/prod as they browse through code

