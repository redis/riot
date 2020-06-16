package com.redislabs.riot.processor;

import com.redislabs.riot.convert.IdemConverter;
import com.redislabs.riot.convert.NullConverter;
import com.redislabs.riot.convert.map.*;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.StreamMessage;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.redis.KeyValue;
import org.springframework.core.convert.converter.Converter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KeyValueItemProcessor<K, V> implements ItemProcessor<KeyValue<K>, Map<K, Object>> {

    private final Converter<K, Map<K, V>> keyFieldsExtractor;
    private final Converter<Map<K, V>, Map<K, V>> hashConverter;
    private final Converter<List<StreamMessage<K, V>>, Map<K, V>> streamConverter;
    private final Converter<List<V>, Map<K, V>> listConverter;
    private final Converter<Set<V>, Map<K, V>> setConverter;
    private final Converter<List<ScoredValue<V>>, Map<K, V>> zsetConverter;
    private final Converter<V, Map<K, V>> stringConverter;
    private final Converter<Object, Map<K, V>> defaultConverter;

    public KeyValueItemProcessor(Converter<K, Map<K, V>> keyFieldsExtractor, Converter<Map<K, V>, Map<K, V>> hashConverter, Converter<List<V>, Map<K, V>> listConverter, Converter<Set<V>, Map<K, V>> setConverter, Converter<List<StreamMessage<K, V>>, Map<K, V>> streamConverter, Converter<V, Map<K, V>> stringConverter, Converter<List<ScoredValue<V>>, Map<K, V>> zsetConverter, Converter<Object, Map<K, V>> defaultConverter) {
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
    public Map<K, Object> process(KeyValue<K> item) {
        Map<K, Object> map = new HashMap<>(keyFieldsExtractor.convert(item.getKey()));
        Map<K, V> valueMap = map(item);
        if (valueMap != null) {
            map.putAll(valueMap);
        }
        return map;
    }

    private Map<K, V> map(KeyValue<K> item) {
        switch (item.getType()) {
            case HASH:
                return hashConverter.convert((Map<K, V>) item.getValue());
            case LIST:
                return listConverter.convert((List<V>) item.getValue());
            case SET:
                return setConverter.convert((Set<V>) item.getValue());
            case ZSET:
                return zsetConverter.convert((List<ScoredValue<V>>) item.getValue());
            case STREAM:
                return streamConverter.convert((List<StreamMessage<K, V>>) item.getValue());
            case STRING:
                return stringConverter.convert((V) item.getValue());
            default:
                return defaultConverter.convert(item.getValue());
        }
    }

    public static KeyValueItemProcessorBuilder builder() {
        return new KeyValueItemProcessorBuilder();
    }

    @Accessors(fluent = true)
    @Setter
    public static class KeyValueItemProcessorBuilder {

        public static final String DEFAULT_KEY_REGEX = "\\w+:(?<id>.+)";

        private String keyRegex = DEFAULT_KEY_REGEX;

        public KeyValueItemProcessor<String, String> build() {
            RegexNamedGroupsExtractor keyFieldsExtractor = RegexNamedGroupsExtractor.builder().regex(keyRegex).build();
            Converter<Map<String, String>, Map<String, String>> hashConverter = new IdemConverter<>();
            StreamToStringMapConverter streamConverter = StreamToStringMapConverter.builder().build();
            CollectionToStringMapConverter<List<String>> listConverter = CollectionToStringMapConverter.<List<String>>builder().build();
            CollectionToStringMapConverter<Set<String>> setConverter = CollectionToStringMapConverter.<Set<String>>builder().build();
            ZsetToStringMapConverter zsetConverter = ZsetToStringMapConverter.builder().build();
            Converter<String, Map<String, String>> stringConverter = StringToStringMapConverter.builder().build();
            Converter<Object, Map<String, String>> defaultConverter = new NullConverter<>();
            return new KeyValueItemProcessor<>(keyFieldsExtractor, hashConverter, listConverter, setConverter, streamConverter, stringConverter, zsetConverter, defaultConverter);
        }

    }
}
