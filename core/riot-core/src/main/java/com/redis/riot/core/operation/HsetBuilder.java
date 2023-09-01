package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Hset;

public class HsetBuilder extends AbstractFilterMapOperationBuilder<HsetBuilder> {

    @Override
    protected Hset<String, String, Map<String, Object>> operation() {
        return new Hset<String, String, Map<String, Object>>().map(map());
    }

}
