package com.redis.riot.core.operation;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.writer.operation.Expire;

public class ExpireBuilder extends AbstractMapOperationBuilder<ExpireBuilder> {

    private String ttl;

    private Duration defaultTtl;

    public ExpireBuilder ttl(String field) {
        this.ttl = field;
        return this;
    }

    public ExpireBuilder defaultTtl(Duration duration) {
        this.defaultTtl = duration;
        return this;
    }

    @Override
    protected Expire<String, String, Map<String, Object>> operation() {
        Expire<String, String, Map<String, Object>> operation = new Expire<>();
        operation.setTtl(ttl());
        return operation;
    }

    private Function<Map<String, Object>, Duration> ttl() {
        ToLongFunction<Map<String, Object>> toLong = toLong(ttl, defaultTtlMillis());
        return t -> Duration.ofMillis(toLong.applyAsLong(t));
    }

    private long defaultTtlMillis() {
        if (defaultTtl == null) {
            return 0;
        }
        return defaultTtl.toMillis();
    }

}
