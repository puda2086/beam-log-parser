package org.example.logscanner.transform;

import avro.shaded.com.google.common.collect.Lists;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.util.List;

public class FormatKVMaps extends SimpleFunction<KV<String, Iterable<String>>, String> {

    @Override
    public String apply(KV<String, Iterable<String>> input) {

        List<String> values = Lists.newArrayList(input.getValue().iterator());

        return input.getKey() + ": [" + String.join(", ", values) + "]";
    }

}
