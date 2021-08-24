package com.redis.riot.redis;

import com.redislabs.mesclun.api.async.RedisModulesAsyncCommands;
import com.redislabs.mesclun.search.SugaddOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import org.springframework.batch.item.redis.support.operation.AbstractKeyOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Sugadd<T> extends AbstractKeyOperation<String, String, T, String> {

    private final Converter<T, Double> score;
    private final Converter<T, String> payload;
    private final boolean increment;

    public Sugadd(Converter<T, String> key, Converter<T, String> string, Converter<T, Double> score, Converter<T, String> payload, boolean increment) {
        super(key, string, t -> false);
        Assert.notNull(string, "A string converter is required");
        Assert.notNull(score, "A score converter is required");
        this.score = score;
        this.payload = payload;
        this.increment = increment;
    }

    @Override
    protected RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item, String key, String value) {
        Double score = this.score.convert(item);
        if (score == null) {
            return null;
        }
        if (payload == null && !increment) {
            return ((RedisModulesAsyncCommands<String, String>) commands).sugadd(key, value, score);
        }
        SugaddOptions<String> options = new SugaddOptions<>();
        options.setIncrement(increment);
        if (payload != null) {
            options.setPayload(payload.convert(item));
        }
        return ((RedisModulesAsyncCommands<String, String>) commands).sugadd(key, value, score, options);
    }

}
