package com.redis.riot.core.operation;

import java.time.Duration;
import java.util.Map;
import java.util.function.Function;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.writer.operation.Expire;

public class ExpireBuilder extends AbstractMapOperationBuilder {

    private String ttlField;

    private Duration defaultTtl;

    public void setTtlField(String ttl) {
        this.ttlField = ttl;
    }

    public void setDefaultTtl(Duration defaultTtl) {
        this.defaultTtl = defaultTtl;
    }

    @Override
    protected Expire<String, String, Map<String, Object>> operation() {
        Expire<String, String, Map<String, Object>> operation = new Expire<>();
        operation.setTtlFunction(ttl());
        return operation;
    }

    private Function<Map<String, Object>, Duration> ttl() {
        ToLongFunction<Map<String, Object>> toLong = toLong(ttlField, defaultTtlMillis());
        return t -> Duration.ofMillis(toLong.applyAsLong(t));
    }

    private long defaultTtlMillis() {
        if (defaultTtl == null) {
            return 0;
        }
        return defaultTtl.toMillis();
    }

}
