package org.example.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.example.entity.LogElement;

@Slf4j
public class ExtractLogMetrics extends PTransform<PCollection<LogElement>, PCollectionList<String>> {

    @Override
    public PCollectionList<String> expand(PCollection<LogElement> input) {

        // Count unique IPs
        PCollection<String> uniqueIpCount = input.apply(new CountUniqueIps());

        // Count URL visits
        PCollection<String> mostVisitedUrls = input.apply(new TopVisitedUrls());

        // Count most active IPs
        PCollection<String> mostActiveIps = input.apply(new TopActiveIps());

        return PCollectionList
                .of(uniqueIpCount)
                .and(mostVisitedUrls)
                .and(mostActiveIps);

    }

}
