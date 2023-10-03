package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Lpush;

public class LpushBuilder extends AbstractCollectionMapOperationBuilder {

    @Override
    protected Lpush<String, String, Map<String, Object>> operation() {
        Lpush<String, String, Map<String, Object>> operation = new Lpush<>();
        operation.setValueFunction(member());
        return operation;
    }

}
