package org.example.logscanner.util;

import lombok.experimental.UtilityClass;
import org.apache.beam.sdk.values.KV;

import java.util.List;
import java.util.stream.Collectors;

@UtilityClass
public class FormatUtils {

    public static List<String> formatValueStringWithCount(List<KV<String, Long>> kvs) {
        return kvs.stream()
                .map(kv -> "%s[%d]".formatted(kv.getKey(), kv.getValue()))
                .collect(Collectors.toList());
    }

}
