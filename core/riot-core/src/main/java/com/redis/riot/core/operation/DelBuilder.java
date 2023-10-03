package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.AbstractKeyWriteOperation;
import com.redis.spring.batch.writer.operation.Del;

public class DelBuilder extends AbstractMapOperationBuilder {

    @Override
    protected AbstractKeyWriteOperation<String, String, Map<String, Object>> operation() {
        return new Del<>();
    }

}
