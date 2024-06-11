package com.redis.riot.function;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import com.redis.lettucemod.search.Suggestion;

public class ToSuggestion<V, T> implements Function<T, Suggestion<V>> {

    private final Function<T, V> string;

    private final ToDoubleFunction<T> score;

    private final Function<T, V> payload;

    public ToSuggestion(Function<T, V> string, ToDoubleFunction<T> score, Function<T, V> payload) {
        this.string = string;
        this.score = score;
        this.payload = payload;
    }

    @Override
    public Suggestion<V> apply(T source) {
        Suggestion<V> suggestion = new Suggestion<>();
        suggestion.setString(string.apply(source));
        suggestion.setScore(score.applyAsDouble(source));
        suggestion.setPayload(payload.apply(source));
        return suggestion;
    }

}
