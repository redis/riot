package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Lpush;

public class LpushBuilder extends AbstractCollectionMapOperationBuilder<LpushBuilder> {

    @Override
    protected Lpush<String, String, Map<String, Object>> operation() {
        return new Lpush<String, String, Map<String, Object>>().value(member());
    }

}
