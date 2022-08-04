# Log Parser

## Introduction

A Beam pipeline that parses logs and outputs metrics.

The pipeline reads from a log file, parses the logs, extracts metrics and outputs the extracted data to an output file.

## Running the Pipeline

The Beam pipeline can run locally using the direct runner. This project uses Maven as build and dependency management
tool.

To execute, run the following:

```shell
mvn compile exec:java \
  -Dexec.mainClass=org.example.PipelineApplication \ 
  -Pdirect-runner
```

## Testing the transforms

Unit tests for transforms can be executed via,

```shell
mvn compile test
```
