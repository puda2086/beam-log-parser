package org.example.logscanner.transform;

import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.logscanner.comparator.CompareByValue;
import org.example.logscanner.entity.LogElement;
import org.example.logscanner.util.FormatUtils;

public class TopVisitedUrls extends PTransform<PCollection<LogElement>, PCollection<String>> {

    @Override
    public PCollection<String> expand(PCollection<LogElement> input) {
        return input
                .apply("Pair with URL key", WithKeys.of(LogElement::getUrl).withKeyType(TypeDescriptor.of(String.class)))
                .apply("Count visits per key", Count.perKey())
                .apply("Top 3 visited URLs", Top.of(3, new CompareByValue()).withoutDefaults())
                .apply("Flatten formatted elements", FlatMapElements
                        .into(TypeDescriptor.of(String.class))
                        .via(FormatUtils::formatValueStringWithCount))
                .apply("To KV", WithKeys.of("topVisitedUrls"))
                .apply("Aggregate results", GroupByKey.create())
                .apply("Format elements", MapElements.via(new FormatKVMaps()));
    }
}
