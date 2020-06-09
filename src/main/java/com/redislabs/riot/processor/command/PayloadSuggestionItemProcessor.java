package com.redislabs.riot.processor.command;

import com.redislabs.lettusearch.suggest.Suggestion;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class PayloadSuggestionItemProcessor<K, V> extends SuggestionItemProcessor<K, V> {

    private final Converter<Map<K, V>, K> payloadConverter;

    public PayloadSuggestionItemProcessor(Converter<Map<K, V>, K> stringConverter, Converter<Map<K, V>, Double> scoreConverter, Converter<Map<K, V>, K> payloadConverter) {
        super(stringConverter, scoreConverter);
        this.payloadConverter = payloadConverter;
    }

    @Override
    protected void setPayload(Suggestion<K> suggestion, Map<K, V> item) {
        suggestion.setPayload(payloadConverter.convert(item));
        super.setPayload(suggestion, item);
    }

}
