package org.example.logscanner.transform;

import avro.shaded.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.example.logscanner.entity.LogElement;
import org.example.logscanner.entity.UserAgent;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class ParseLogTransformTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void givenValidLogMessage_whenParseLogIsApplied_thenOutputLogElement() {

        // Mock
        List<String> logInputs = ImmutableList.of(
                "177.71.128.21 - - [10/Jul/2018:22:21:28 +0200] \"GET /intranet-analytics/ HTTP/1.1\" 200 3574 \"-\" \"Mozilla/5.0 (X11; U; Linux x86_64; fr-FR) AppleWebKit/534.7 (KHTML, like Gecko)\""
        );

        LogElement expectedLogElement = LogElement.builder()
                .ipAddress("177.71.128.21")
                .url("/intranet-analytics/")
                .user("-")
                .group("-")
                .status(200)
                .userAgents(List.of(
                        UserAgent.builder()
                                .name("Mozilla")
                                .version("5.0")
                                .info("X11; U; Linux x86_64; fr-FR")
                                .build(),
                        UserAgent.builder()
                                .name("AppleWebKit")
                                .version("534.7")
                                .info("KHTML, like Gecko")
                                .build()
                ))
                .build();

        // Invoke
        PCollection<String> input = testPipeline.apply(Create.of(logInputs));

        PCollection<LogElement> output = input.apply("Parse logs", ParDo.of(new ParseLog()));

        // Assert
        PAssert.that(output).containsInAnyOrder(
                expectedLogElement
        );

        testPipeline.run().waitUntilFinish();
    }

    @Test
    public void givenInvalidLogMessage_whenParseLogIsApplied_thenOutputEmpty() {

        // Mock
        List<String> logInputs = ImmutableList.of(
                "Invalid input"
        );

        // Invoke
        PCollection<String> input = testPipeline.apply(Create.of(logInputs));

        PCollection<LogElement> output = input.apply("Parse logs", ParDo.of(new ParseLog()));

        // Assert
        PAssert.that(output).empty();

        testPipeline.run().waitUntilFinish();
    }

}