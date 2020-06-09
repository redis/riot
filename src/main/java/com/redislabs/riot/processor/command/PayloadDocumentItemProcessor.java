package com.redislabs.riot.processor.command;

import com.redislabs.lettusearch.search.Document;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class PayloadDocumentItemProcessor<K, V> extends DocumentItemProcessor<K, V> {

    private final Converter<Map<K, V>, V> payloadConverter;

    public PayloadDocumentItemProcessor(Converter<Map<K, V>, K> idConverter, Converter<Map<K, V>, Double> scoreConverter, Converter<Map<K, V>, V> payloadConverter) {
        super(idConverter, scoreConverter);
        this.payloadConverter = payloadConverter;
    }

    @Override
    protected void setPayload(Document<K, V> document, Map<K, V> item) {
        document.setPayload(payloadConverter.convert(item));
        super.setPayload(document, item);
    }

}
