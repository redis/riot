package com.redis.riot.processor;

import com.redis.riot.convert.*;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

import java.util.*;

public class DataStructureItemProcessor implements ItemProcessor<DataStructure, Map<String, Object>> {

    private final Converter<String, Map<String, String>> keyFieldsExtractor;
    private final Converter<Map<String, String>, Map<String, String>> hashConverter;
    private final Converter<List<StreamMessage<String, String>>, Map<String, String>> streamConverter;
    private final Converter<Collection<String>, Map<String, String>> listConverter;
    private final Converter<Collection<String>, Map<String, String>> setConverter;
    private final Converter<List<ScoredValue<String>>, Map<String, String>> zsetConverter;
    private final Converter<String, Map<String, String>> stringConverter;
    private final Converter<Object, Map<String, String>> defaultConverter;

    public DataStructureItemProcessor(Converter<String, Map<String, String>> keyFieldsExtractor, Converter<Map<String, String>, Map<String, String>> hashConverter, Converter<Collection<String>, Map<String, String>> listConverter, Converter<Collection<String>, Map<String, String>> setConverter, Converter<List<StreamMessage<String, String>>, Map<String, String>> streamConverter, Converter<String, Map<String, String>> stringConverter, Converter<List<ScoredValue<String>>, Map<String, String>> zsetConverter, Converter<Object, Map<String, String>> defaultConverter) {
        this.keyFieldsExtractor = keyFieldsExtractor;
        this.hashConverter = hashConverter;
        this.listConverter = listConverter;
        this.setConverter = setConverter;
        this.streamConverter = streamConverter;
        this.stringConverter = stringConverter;
        this.zsetConverter = zsetConverter;
        this.defaultConverter = defaultConverter;
    }

    @Override
    public Map<String, Object> process(DataStructure item) {
        if (item.getType() == null) {
            return null;
        }
        if (item.getKey() == null) {
            return null;
        }
        Map<String, String> stringMap = keyFieldsExtractor.convert(item.getKey());
        if (stringMap == null) {
            return null;
        }
        Map<String, Object> map = new HashMap<>(stringMap);
        Map<String, String> valueMap = map(item);
        if (valueMap != null) {
            map.putAll(valueMap);
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> map(DataStructure item) {
        switch (item.getType()) {
            case DataStructure.HASH:
                return hashConverter.convert((Map<String, String>) item.getValue());
            case DataStructure.LIST:
                return listConverter.convert((List<String>) item.getValue());
            case DataStructure.SET:
                return setConverter.convert((Set<String>) item.getValue());
            case DataStructure.ZSET:
                return zsetConverter.convert((List<ScoredValue<String>>) item.getValue());
            case DataStructure.STREAM:
                return streamConverter.convert((List<StreamMessage<String, String>>) item.getValue());
            case DataStructure.STRING:
                return stringConverter.convert((String) item.getValue());
            default:
                return defaultConverter.convert(item.getValue());
        }
    }

    public static DataStructureItemProcessorBuilder builder() {
        return new DataStructureItemProcessorBuilder();
    }

    @Setter
    @Accessors(fluent = true)
    public static class DataStructureItemProcessorBuilder {

        private String keyRegex;

        public DataStructureItemProcessor build() {
            Assert.notNull(keyRegex, "Key regex is required.");
            RegexNamedGroupsExtractor keyFieldsExtractor = RegexNamedGroupsExtractor.builder().regex(keyRegex).build();
            StreamToStringMapConverter streamConverter = StreamToStringMapConverter.builder().build();
            CollectionToStringMapConverter listConverter = CollectionToStringMapConverter.builder().build();
            CollectionToStringMapConverter setConverter = CollectionToStringMapConverter.builder().build();
            ZsetToStringMapConverter zsetConverter = new ZsetToStringMapConverter();
            Converter<String, Map<String, String>> stringConverter = new StringToStringMapConverter();
            return new DataStructureItemProcessor(keyFieldsExtractor, c -> c, listConverter, setConverter, streamConverter, stringConverter, zsetConverter, c -> null);
        }

    }


}
