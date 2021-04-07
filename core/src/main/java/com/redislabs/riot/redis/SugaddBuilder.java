package com.redislabs.riot.redis;

import com.redislabs.lettusearch.SuggestAsyncCommands;
import com.redislabs.lettusearch.Suggestion;
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

    @SuppressWarnings("unchecked")
    @Override
    protected RedisOperation<String, String, T> build(Converter<T, String> keyConverter) {
        return (c, t) -> ((SuggestAsyncCommands<String, String>) c).sugadd(keyConverter.convert(t), suggestion(t));
    }

    private Suggestion<String> suggestion(T value) {
        Suggestion.SuggestionBuilder<String> suggestion = Suggestion.builder();
        suggestion.string(stringConverter.convert(value));
        Double score = scoreConverter.convert(value);
        if (score != null) {
            suggestion.score(score);
        }
        if (payloadConverter != null) {
            suggestion.payload(payloadConverter.convert(value));
        }
        return suggestion.build();
    }

}