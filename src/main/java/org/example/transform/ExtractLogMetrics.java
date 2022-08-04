package org.example.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.example.comparator.CompareByValue;
import org.example.entity.LogElement;

import java.util.stream.Collectors;

@Slf4j
public class ExtractLogMetrics extends PTransform<PCollection<LogElement>, PCollectionList<String>> {

    @Override
    public PCollectionList<String> expand(PCollection<LogElement> input) {

        PCollection<KV<String, LogElement>> logsByIp = input.apply(WithKeys.of(LogElement::getIpAddress)
                .withKeyType(TypeDescriptor.of(String.class)));

        // Count unique IPs
        PCollection<String> uniqueIpCount = logsByIp
                .apply("Map to IP addresses", Keys.create())
                .apply("Count unique entries", Count.globally())
                .apply("To KV", WithKeys.of("uniqueIpCount"))
                .apply("Format elements", MapElements.via(new FormatKV()));

        // Count URL visits
        PCollection<String> mostVisitedUrls = input
                .apply("Group by URL", WithKeys.of(LogElement::getUrl)
                        .withKeyType(TypeDescriptor.of(String.class)))
                .apply("Count visits per key", Count.perKey())
                .apply("Top 3 visited URLs", Top.of(3, new CompareByValue()).withoutDefaults())
                .apply("Flatten elements", FlatMapElements
                        .into(TypeDescriptor.of(String.class))
                        .via(kvs -> kvs.stream()
                                .map(KV::getKey)
                                .collect(Collectors.toList())))
                .apply("To KV", WithKeys.of("topVisitedUrls"))
                .apply("Aggregate results", GroupByKey.create())
                .apply("Format elements", MapElements.via(new FormatKVMaps()));

        // Count most active IPs
        PCollection<String> mostActiveIps = logsByIp
                .apply("Count IP activity", Count.perKey())
                .apply("Top 3 most active IPs", Top.of(3, new CompareByValue()).withoutDefaults())
                .apply("Flatten elements", FlatMapElements
                        .into(TypeDescriptor.of(String.class))
                        .via(kvs -> kvs.stream()
                                .map(KV::getKey)
                                .collect(Collectors.toList())))
                .apply("To KV", WithKeys.of("topActiveIps"))
                .apply("Aggregate results", GroupByKey.create())
                .apply("Format elements", MapElements.via(new FormatKVMaps()));

        return PCollectionList.of(uniqueIpCount)
                .and(mostVisitedUrls)
                .and(mostActiveIps);

    }

}
