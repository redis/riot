package com.redislabs.riot.redis;

import com.redislabs.mesclun.api.async.RedisModulesAsyncCommands;
import com.redislabs.mesclun.search.SugaddOptions;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.BaseRedisAsyncCommands;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.operation.AbstractKeyOperation;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public class Sugadd<T> extends AbstractKeyOperation<T> {

    private final Converter<T, String> string;
    private final Converter<T, Double> score;
    private final Converter<T, String> payload;
    private final boolean increment;

    public Sugadd(Converter<T, String> key, Converter<T, String> string, Converter<T, Double> score, Converter<T, String> payload, boolean increment) {
        super(key);
        Assert.notNull(string, "A string converter is required");
        Assert.notNull(score, "A score converter is required");
        this.string = string;
        this.score = score;
        this.payload = payload;
        this.increment = increment;
    }

    @Override
    public RedisFuture<?> execute(BaseRedisAsyncCommands<String, String> commands, T item) {
        Double score = this.score.convert(item);
        if (payload == null && !increment) {
            return ((RedisModulesAsyncCommands<String, String>) commands).sugadd(key.convert(item), string.convert(item), score);
        }
        SugaddOptions.SugaddOptionsBuilder options = SugaddOptions.builder();
        options.increment(increment);
        if (payload != null) {
            options.payload(payload.convert(item));
        }
        return ((RedisModulesAsyncCommands<String, String>) commands).sugadd(key.convert(item), string.convert(item), score, options.build());
    }

    public static <T> SugaddBuilder<T> builder() {
        return new SugaddBuilder<>();
    }

    @Setter
    @Accessors(fluent = true)
    public static class SugaddBuilder<T> extends KeyOperationBuilder<T, SugaddBuilder<T>> {

        private Converter<T, String> string;
        private Converter<T, Double> score;
        private Converter<T, String> payload;
        private boolean increment;

        public Sugadd<T> build() {
            return new Sugadd<>(key, string, score, payload, increment);
        }


    }

}
