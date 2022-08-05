package org.example.logscanner.comparator;

import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.values.KV;

public class CompareByValue implements SerializableComparator<KV<String, Long>> {

    @Override
    public int compare(KV<String, Long> o1, KV<String, Long> o2) {
        return (int) (o1.getValue() - o2.getValue());
    }

}
