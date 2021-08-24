package com.redis.riot.gen;

import java.util.Map;

public class MapWithMetadataGenerator implements Generator<Map<String, Object>> {

    private final static String FIELD_THREAD = "thread";

    private final Generator<Map<String, Object>> parent;

    public MapWithMetadataGenerator(Generator<Map<String, Object>> parent) {
        this.parent = parent;
    }

    @Override
    public Map<String, Object> next(long index) {
        Map<String, Object> map = parent.next(index);
        map.put(MapGenerator.FIELD_INDEX, index);
        map.put(FIELD_THREAD, Thread.currentThread().getId());
        return map;
    }
}
