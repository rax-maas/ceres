# 3. Ensure Unit tests for Configuration Files

Date: 2022-04-04

## Status

Accepted

triggered by [2. Store Per Environment Configuration Files in Ceres Repo](0002-store-per-environment-configuration-files-in-ceres-repo.md)

## Context

Configuration files used for deployments needs to be valid to prevent runtime errors.

## Decision

Write unit tests that verifies the validity of each configuration file content
Examples: 
- If a property needs to be numeric, tests should cover it
- downsample-process-period needs to be multiple of 16

## Consequences

Prevents bad releases due to invalid configuration settings
