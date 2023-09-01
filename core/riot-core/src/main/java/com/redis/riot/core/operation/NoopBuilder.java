package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.Operation;
import com.redis.spring.batch.writer.operation.Noop;

public class NoopBuilder implements OperationBuilder {

    @Override
    public Operation<String, String, Map<String, Object>> build() {
        return new Noop<>();
    }

}
