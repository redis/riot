package com.redis.riot.core.operation;

import java.util.Map;

import com.redis.spring.batch.writer.operation.ExpireAt;

public class ExpireAtBuilder extends AbstractMapOperationBuilder<ExpireAtBuilder> {

    private String ttl;

    public ExpireAtBuilder ttl(String field) {
        this.ttl = field;
        return this;
    }

    @Override
    protected ExpireAt<String, String, Map<String, Object>> operation() {
        return new ExpireAt<String, String, Map<String, Object>>().epoch(toLong(ttl, 0));
    }

}
