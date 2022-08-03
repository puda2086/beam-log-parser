package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.example.transform.ExtractLogMetrics;
import org.example.transform.ParseLogTransform;

public class PipelineApplication {

    public static void main(String[] args) {

        PipelineOptions options = PipelineOptionsFactory.create();

        setupPipeline(options)
                .run().waitUntilFinish();
    }

    private static Pipeline setupPipeline(PipelineOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Read logs", TextIO.read().from("src/main/resources/programming-task-example-data.log"))
                .apply("Extract logs", ParDo.of(new ParseLogTransform()));

        return pipeline;
    }
}
