package org.example.logscanner.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface LogScannerOptions extends PipelineOptions {

    @Description("Filename or file pattern to read logs from.")
    @Validation.Required
    String getInputFile();

    void setInputFile(String value);

    @Description("Filename prefix to which the output is written. The output file will have the suffix `.out`")
    @Default.String("log_report")
    String getOutputPrefix();

    void setOutputPrefix(String prefix);
}
