package com.redislabs.riot.processor.command;

import com.redislabs.lettusearch.suggest.Suggestion;
import lombok.Builder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class SuggestionProcessor<K, V> implements ItemProcessor<Map<K, V>, Suggestion<K>> {

    private final Converter<Map<K, V>, K> stringConverter;
    private final Converter<Map<K, V>, Double> scoreConverter;
    private final Converter<Map<K, V>, K> payloadConverter;

    @Builder
    public SuggestionProcessor(Converter<Map<K, V>, K> stringConverter, Converter<Map<K, V>, Double> scoreConverter, Converter<Map<K, V>, K> payloadConverter) {
        this.stringConverter = stringConverter;
        this.scoreConverter = scoreConverter;
        this.payloadConverter = payloadConverter;
    }

    @Override
    public Suggestion<K> process(Map<K, V> item) {
        Double score = scoreConverter.convert(item);
        if (score == null) {
            return null;
        }
        return new Suggestion<>(stringConverter.convert(item), score, payloadConverter.convert(item));
    }

}
