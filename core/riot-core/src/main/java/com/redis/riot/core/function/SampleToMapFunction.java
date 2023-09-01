package com.redis.riot.core.function;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import com.redis.lettucemod.timeseries.Sample;

public class SampleToMapFunction implements Function<List<Sample>, Map<String, String>> {

    @Override
    public Map<String, String> apply(List<Sample> source) {
        Map<String, String> result = new HashMap<>();
        for (int index = 0; index < source.size(); index++) {
            Sample sample = source.get(index);
            result.put(String.valueOf(index), sample.getTimestamp() + ":" + sample.getValue());
        }
        return result;
    }

}
