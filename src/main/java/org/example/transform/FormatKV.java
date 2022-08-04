package org.example.transform;


import lombok.RequiredArgsConstructor;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;

@RequiredArgsConstructor
public class FormatKV extends SimpleFunction<KV<String, ? extends Serializable>, String> {

    @Override
    public String apply(KV<String, ? extends Serializable> input) {
        return input.getKey() + ": " + input.getValue();
    }

}
