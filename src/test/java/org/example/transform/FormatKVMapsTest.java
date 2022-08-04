package org.example.transform;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

public class FormatKVMapsTest {

    @Rule
    public final transient TestPipeline testPipeline = TestPipeline.create();

    @Test
    public void givenValidKVWithValueIterable_whenFormatting_thenOutputFormattedString() {

        // Mock
        KV<String, Iterable<String>> inputKV = KV.of("testKey", List.of("value1", "value2"));

        String expectedOutput = "testKey: [value1, value2]";

        // Invoke
        PCollection<KV<String, Iterable<String>>> input = testPipeline.apply("Create input", Create.of(inputKV));

        PCollection<String> output = input.apply("Format KV with iterable", MapElements.via(new FormatKVMaps()));

        // Assert
        PAssert.that(output).containsInAnyOrder(expectedOutput);

        testPipeline.run().waitUntilFinish();

    }

}