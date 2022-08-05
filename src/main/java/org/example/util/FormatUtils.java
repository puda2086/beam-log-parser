package org.example.util;

import org.apache.beam.sdk.values.KV;

import java.util.List;
import java.util.stream.Collectors;

public class FormatUtils {
    public static List<String> formatValueStringWithCount(List<KV<String, Long>> kvs) {
        return kvs.stream()
                .map(kv -> "%s[%d]".formatted(kv.getKey(), kv.getValue()))
                .collect(Collectors.toList());
    }
}
