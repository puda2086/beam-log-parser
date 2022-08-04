package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.example.transform.ExtractLogMetrics;
import org.example.transform.ParseLog;

public class PipelineApplication {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        setupPipeline(options)
                .run().waitUntilFinish();
    }

    private static Pipeline setupPipeline(PipelineOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read logs", TextIO.read()
                        .from("src/main/resources/*.log"))
                .apply("Extract logs", ParDo.of(new ParseLog()))
                .apply("Collect metrics", new ExtractLogMetrics())
                .apply("Flatten output", Flatten.pCollections())
                .apply(TextIO.write()
                        .to("output")
                        .withoutSharding());

        return pipeline;
    }
}
