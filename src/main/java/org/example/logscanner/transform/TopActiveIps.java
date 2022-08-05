package org.example.logscanner.transform;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.logscanner.comparator.CompareByValue;
import org.example.logscanner.entity.LogElement;
import org.example.logscanner.util.FormatUtils;

public class TopActiveIps extends PTransform<PCollection<LogElement>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<LogElement> input) {
        return input
                .apply("Pair with IP key", WithKeys.of(LogElement::getIpAddress).withKeyType(TypeDescriptor.of(String.class)))
                .apply("Count IP activity", Count.perKey())
                .apply("Top 3 most active IPs", Top.of(3, new CompareByValue()).withoutDefaults())
                .apply("Flatten formatted elements", FlatMapElements
                        .into(TypeDescriptor.of(String.class))
                        .via(FormatUtils::formatValueStringWithCount))
                .apply("To KV", WithKeys.of("topActiveIps"))
                .apply("Aggregate results", GroupByKey.create())
                .apply("Format elements", MapElements.via(new FormatKVMaps()));
    }
}
