package org.example.transform;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class FormatKVTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void givenValidKV_whenFormatting_thenOutputFormattedString() {

        // Mock
        KV<String, String> inputKV = KV.of("testKey", "testValue");

        String expectedOutput = "testKey: testValue";

        // Invoke
        PCollection<KV<String, String>> input = testPipeline.apply("Create input", Create.of(inputKV));

        PCollection<String> output = input.apply("Format KV", MapElements.via(new FormatKV()));

        // Assert
        PAssert.that(output).containsInAnyOrder(expectedOutput);

        testPipeline.run().waitUntilFinish();

    }
}