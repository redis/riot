package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Sadd;

public class SaddBuilder extends AbstractCollectionMapOperationBuilder<SaddBuilder> {

    @Override
    protected Sadd<String, String, Map<String, Object>> operation() {
        return new Sadd<String, String, Map<String, Object>>().value(member());
    }

}
