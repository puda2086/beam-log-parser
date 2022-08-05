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
  -Dexec.mainClass=PipelineApplication \
  -Dexec.args="--inputFile=<input_file_path> [--outputPrefix=<output_file_prefix>]"
  -Pdirect-runner
```

The following arguments are accepted when running the application.

* `--inputFile`Specifies the input log file name or file pattern.
* `--outputPrefix` Filename prefix to which the output is written. The output file will have the suffix `.out`
  . <br/>If an output file is not specified, the report will be written to a file titled `log_report.out`.

**NOTE:** A maven wrapper is included for easier execution of the application. Simply replace `mvn` with `./mvnw` for
Linux systems or `mvnw.cmd` for Windows.

## Testing the transforms

Unit tests for transforms can be executed via,

```shell
mvn compile test
```
