package org.example.logscanner.transform;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.logscanner.entity.LogElement;

public class CountUniqueIps extends PTransform<PCollection<LogElement>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<LogElement> input) {
        return input
                .apply("Pair with IP key", WithKeys.of(LogElement::getIpAddress).withKeyType(TypeDescriptor.of(String.class)))
                .apply("Group by IP", GroupByKey.create())
                .apply("Get IP addresses", Keys.create())
                .apply("Count unique entries", Count.globally())
                .apply("To KV", WithKeys.of("uniqueIpCount"))
                .apply("Format elements", MapElements.via(new FormatKV()));
    }
}
