package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.Sadd;

public class SaddBuilder extends AbstractCollectionMapOperationBuilder {

    @Override
    protected Sadd<String, String, Map<String, Object>> operation() {
        Sadd<String, String, Map<String, Object>> operation = new Sadd<>();
        operation.setValueFunction(member());
        return operation;
    }

}
