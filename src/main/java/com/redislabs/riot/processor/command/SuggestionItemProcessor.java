package com.redislabs.riot.processor.command;

import com.redislabs.lettusearch.suggest.Suggestion;
import lombok.Builder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class SuggestionItemProcessor<K, V> implements ItemProcessor<Map<K, V>, Suggestion<K>> {

    private final Converter<Map<K, V>, K> stringConverter;
    private final Converter<Map<K, V>, Double> scoreConverter;

    @Builder
    public SuggestionItemProcessor(Converter<Map<K, V>, K> stringConverter, Converter<Map<K, V>, Double> scoreConverter) {
        this.stringConverter = stringConverter;
        this.scoreConverter = scoreConverter;
    }

    @Override
    public Suggestion<K> process(Map<K, V> item) {
        Double score = scoreConverter.convert(item);
        if (score == null) {
            return null;
        }
        Suggestion<K> suggestion = new Suggestion<>();
        suggestion.setString(stringConverter.convert(item));
        suggestion.setScore(score);
        setPayload(suggestion, item);
        return suggestion;
    }

    protected void setPayload(Suggestion<K> suggestion, Map<K, V> item) {
    }

}
