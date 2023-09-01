package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.Operation;

public interface OperationBuilder {

    Operation<String, String, Map<String, Object>> build();

}
