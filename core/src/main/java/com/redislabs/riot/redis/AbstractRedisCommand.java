package com.redislabs.riot.redis;

import com.redislabs.riot.HelpCommand;
import com.redislabs.riot.RedisCommand;
import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.FieldExtractor;
import com.redislabs.riot.convert.ObjectToNumberConverter;
import com.redislabs.riot.convert.ObjectToStringConverter;
import org.springframework.batch.item.redis.support.KeyMaker;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;

import java.util.Map;

@CommandLine.Command(sortOptions = false, abbreviateSynopsis = true)
public abstract class AbstractRedisCommand<O> extends HelpCommand implements RedisCommand<O> {

    @CommandLine.Option(names = {"-s", "--separator"}, description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String keySeparator = ":";
    @SuppressWarnings("unused")
    @CommandLine.Option(names = {"-r", "--remove"}, description = "Remove key or member fields the first time they are used")
    private boolean removeFields;

    protected Converter<Map<String, Object>, Double> doubleFieldExtractor(String field) {
        return numberFieldExtractor(Double.class, field, null);
    }

    protected Converter<Map<String, Object>, Object> fieldExtractor(String field, Object defaultValue) {
        return FieldExtractor.builder().field(field).remove(removeFields).defaultValue(defaultValue).build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Converter<Map<String, Object>, String> stringFieldExtractor(String field) {
        Converter<Map<String, Object>, String> extractor = (Converter) fieldExtractor(field, null);
        if (extractor == null) {
            return null;
        }
        return new CompositeConverter(extractor, new ObjectToStringConverter());
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number> Converter<Map<String, Object>, T> numberFieldExtractor(Class<T> targetType, String field, T defaultValue) {
        Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, defaultValue);
        return new CompositeConverter(extractor, new ObjectToNumberConverter<>(targetType));
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    protected KeyMaker<Map<String, Object>> idMaker(String prefix, String[] fields) {
        KeyMaker.KeyMakerBuilder<Map<String, Object>> builder = KeyMaker.<Map<String, Object>>builder().separator(keySeparator).prefix(prefix);
        if (!ObjectUtils.isEmpty(fields)) {
            Converter[] converters = new Converter[fields.length];
            for (int index = 0; index < fields.length; index++) {
                Converter<Map<String, Object>, Object> extractor = FieldExtractor.builder().remove(removeFields).field(fields[index]).build();
                CompositeConverter converter = new CompositeConverter(extractor, new ObjectToStringConverter());
                converters[index] = converter;
            }
            builder.converters(converters);
        }
        return builder.build();
    }

}
