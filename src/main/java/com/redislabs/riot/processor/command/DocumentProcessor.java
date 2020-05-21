package com.redislabs.riot.processor.command;

import com.redislabs.lettusearch.search.Document;
import lombok.Builder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class DocumentProcessor<K, V> implements ItemProcessor<Map<K, V>, Document<K, V>> {

    private final Converter<Map<K, V>, K> idConverter;
    private final Converter<Map<K, V>, Double> scoreConverter;
    private final Converter<Map<K, V>, V> payloadConverter;

    @Builder
    public DocumentProcessor(Converter<Map<K, V>, K> idConverter, Converter<Map<K, V>, Double> scoreConverter, Converter<Map<K, V>, V> payloadConverter) {
        this.idConverter = idConverter;
        this.scoreConverter = scoreConverter;
        this.payloadConverter = payloadConverter;
    }

    @Override
    public Document<K, V> process(Map<K, V> item) {
        Document<K, V> document = new Document<>(idConverter.convert(item), scoreConverter.convert(item), payloadConverter.convert(item));
        document.putAll(item);
        return document;
    }

}
