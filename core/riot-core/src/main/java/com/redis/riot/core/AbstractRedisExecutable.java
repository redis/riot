package com.redis.riot.core;

public abstract class AbstractRedisExecutable implements RiotExecutable {

    private RedisOptions redisClientOptions = new RedisOptions();

    public RedisOptions getRedisClientOptions() {
        return redisClientOptions;
    }

    public void setRedisClientOptions(RedisOptions redisClientOptions) {
        this.redisClientOptions = redisClientOptions;
    }

    @Override
    public void execute() {
        try (RiotExecutionContext executionContext = executionContext()) {
            execute(executionContext);
        }
    }

    private RiotExecutionContext executionContext() {
        return executionContext(redisClientOptions);
    }

    protected RiotExecutionContext executionContext(RedisOptions redisClientOptions) {
        return new RiotExecutionContext(redisClientOptions);
    }

    protected abstract void execute(RiotExecutionContext executionContext);

}
