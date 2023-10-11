package com.redis.riot.core;

public class ReplicationContext extends RiotContext {

    private final RedisContext targetRedisContext;

    public ReplicationContext(RiotContext context, RedisContext target) {
        super(context.getRedisContext(), context.getEvaluationContext());
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
