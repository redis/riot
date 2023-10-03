package com.redis.riot.core;

import org.springframework.expression.spel.support.StandardEvaluationContext;

public class ReplicationContext extends RiotContext {

    private final RedisContext targetRedisContext;

    public ReplicationContext(RedisContext source, StandardEvaluationContext evaluationContext, RedisContext target) {
        super(source, evaluationContext);
        this.targetRedisContext = target;
    }

    public RedisContext getTargetRedisContext() {
        return targetRedisContext;
    }

    @Override
    public void close() {
        try {
            targetRedisContext.close();
        } finally {
            super.close();
        }
    }

}
