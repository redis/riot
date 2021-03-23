package com.redislabs.riot.redis;

import com.redislabs.lettusearch.SuggestAsyncCommands;
import com.redislabs.lettusearch.Suggestion;
import io.lettuce.core.RedisFuture;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.redis.support.CommandBuilder;
import org.springframework.core.convert.converter.Converter;

import java.util.function.BiFunction;

@Setter
@Accessors(fluent = true)
class SugaddBuilder<T> extends CommandBuilder.KeyCommandBuilder<SuggestAsyncCommands<String, String>, T, SugaddBuilder<T>> {

    @NonNull
    private Converter<T, String> stringConverter;
    @NonNull
    private Converter<T, Double> scoreConverter;
    private Converter<T, String> payloadConverter;

    public BiFunction<SuggestAsyncCommands<String, String>, T, RedisFuture<?>> build() {
        return (c, t) -> c.sugadd(key(t), suggestion(t));
    }

    private Suggestion<String> suggestion(T value) {
        Suggestion.SuggestionBuilder<String> suggestion = Suggestion.builder();
        suggestion.string(stringConverter.convert(value));
        suggestion.score(scoreConverter.convert(value));
        if (payloadConverter != null) {
            suggestion.payload(payloadConverter.convert(value));
        }
        return suggestion.build();
    }

}