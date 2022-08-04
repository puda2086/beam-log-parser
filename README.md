# Log Parser

## Introduction

A Beam pipeline that parses logs and outputs metrics.

## Running the Pipeline

The Beam pipeline can run locally using the direct runner. This project uses Maven as build and dependency management
tool.

To execute, run the following:

```shell
mvn compile exec:java \
  -Dexec.mainClass=org.example.PipelineApplication \ 
  -Pdirect-runner
```