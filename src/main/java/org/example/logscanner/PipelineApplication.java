package org.example.logscanner;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.example.logscanner.options.LogScannerOptions;
import org.example.logscanner.transform.ExtractLogMetrics;
import org.example.logscanner.transform.ParseLog;

public class PipelineApplication {

    public static void main(String[] args) {

        LogScannerOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(LogScannerOptions.class);

        setupPipeline(options)
                .run().waitUntilFinish();
    }

    private static Pipeline setupPipeline(LogScannerOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read logs", TextIO.read()
                        .from(options.getInputFile()))
                .apply("Extract logs", ParDo.of(new ParseLog()))
                .apply("Collect metrics", new ExtractLogMetrics())
                .apply("Flatten output", Flatten.pCollections())
                .apply(TextIO.write()
                        .to(options.getOutputPrefix())
                        .withSuffix(".out")
                        .withoutSharding());

        return pipeline;
    }
}
