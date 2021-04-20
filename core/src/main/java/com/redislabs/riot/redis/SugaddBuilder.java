package com.redislabs.riot.redis;

import com.redislabs.mesclun.RedisModulesAsyncCommands;
import com.redislabs.mesclun.search.SugaddOptions;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.RedisOperationBuilder;
import org.springframework.core.convert.converter.Converter;

@Setter
@Accessors(fluent = true)
class SugaddBuilder<T> extends RedisOperationBuilder.AbstractKeyOperationBuilder<String, String, T, SugaddBuilder<T>> {

    private Converter<T, String> stringConverter;
    private Converter<T, Double> scoreConverter;
    private Converter<T, String> payloadConverter;
    private boolean increment;

    @Override
    protected RedisOperation<String, String, T> build(Converter<T, String> keyConverter) {
        if (payloadConverter == null && !increment) {
            return (c, t) -> ((RedisModulesAsyncCommands<String, String>) c).sugadd(keyConverter.convert(t), stringConverter.convert(t), scoreConverter.convert(t));
        }
        return (c, t) -> {
            Double score = scoreConverter.convert(t);
            if (score == null) {
                return null;
            }
            return ((RedisModulesAsyncCommands<String, String>) c).sugadd(keyConverter.convert(t), stringConverter.convert(t), score, sugaddOptions(t));
        };
    }

    private SugaddOptions<String, String> sugaddOptions(T item) {
        SugaddOptions<String, String> options = new SugaddOptions<>();
        options.setIncrement(increment);
        if (payloadConverter != null) {
            options.setPayload(payloadConverter.convert(item));
        }
        return options;
    }

}