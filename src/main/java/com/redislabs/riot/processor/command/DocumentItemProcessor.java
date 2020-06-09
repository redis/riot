package com.redislabs.riot.processor.command;

import com.redislabs.lettusearch.search.Document;
import lombok.Builder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class DocumentItemProcessor<K, V> implements ItemProcessor<Map<K, V>, Document<K, V>> {

    private final Converter<Map<K, V>, K> idConverter;
    private final Converter<Map<K, V>, Double> scoreConverter;

    @Builder
    public DocumentItemProcessor(Converter<Map<K, V>, K> idConverter, Converter<Map<K, V>, Double> scoreConverter) {
        this.idConverter = idConverter;
        this.scoreConverter = scoreConverter;
    }

    @Override
    public Document<K, V> process(Map<K, V> item) {
        Double score = scoreConverter.convert(item);
        if (score == null) {
            return null;
        }
        Document<K, V> document = new Document<>();
        document.setId(idConverter.convert(item));
        document.setScore(score);
        setPayload(document, item);
        document.putAll(item);
        return document;
    }

    protected void setPayload(Document<K, V> document, Map<K, V> item) {
    }

}
