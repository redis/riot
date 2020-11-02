package com.redislabs.riot.redis;

import java.util.Map;

import org.springframework.batch.item.redis.support.AbstractRedisItemWriter;
import org.springframework.batch.item.redis.support.KeyMaker;
import org.springframework.batch.item.redis.support.RedisConnectionBuilder;
import org.springframework.core.convert.converter.Converter;

import com.redislabs.riot.AbstractImportCommand;
import com.redislabs.riot.HelpCommand;
import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.MapFieldExtractor;
import com.redislabs.riot.convert.ObjectToNumberConverter;
import com.redislabs.riot.convert.ObjectToStringConverter;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(sortOptions = false, abbreviateSynopsis = true)
public abstract class AbstractRedisCommand<O> extends HelpCommand {

    @ParentCommand
    private AbstractImportCommand<?, O> parentCommand;

    @Option(names = { "-s", "--separator" }, description = "Key separator (default: ${DEFAULT-VALUE})", paramLabel = "<str>")
    private String keySeparator = ":";

    @Option(names = { "-r", "--remove" }, description = "Remove fields the first time they are used (key or member fields)")
    private boolean removeFields;

    public abstract AbstractRedisItemWriter<String, String, O> writer() throws Exception;

    protected <B extends RedisConnectionBuilder<String, String, B>> B configure(B builder) throws Exception {
        return parentCommand.configure(builder);
    }

    protected Converter<Map<String, Object>, Double> doubleFieldExtractor(String field) {
        return numberFieldExtractor(Double.class, field, null);
    }

    protected Converter<Map<String, Object>, Object> fieldExtractor(String field, Object defaultValue) {
        return MapFieldExtractor.builder().field(field).remove(removeFields).defaultValue(defaultValue).build();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Converter<Map<String, Object>, String> stringFieldExtractor(String field) {
        Converter<Map<String, Object>, String> extractor = (Converter) fieldExtractor(field, null);
        if (extractor == null) {
            return null;
        }
        return new CompositeConverter(extractor, new ObjectToStringConverter());
    }

    @SuppressWarnings("unchecked")
    protected <T extends Number> Converter<Map<String, Object>, T> numberFieldExtractor(Class<T> targetType, String field,
            T defaultValue) {
        Converter<Map<String, Object>, Object> extractor = fieldExtractor(field, defaultValue);
        return new CompositeConverter(extractor, new ObjectToNumberConverter<>(targetType));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected KeyMaker<Map<String, Object>> idMaker(String prefix, String[] fields) {
        Converter[] extractors = new Converter[fields.length];
        for (int index = 0; index < fields.length; index++) {
            extractors[index] = MapFieldExtractor.builder().remove(removeFields).field(fields[index]).build();
        }
        return KeyMaker.<Map<String, Object>> builder().separator(keySeparator).prefix(prefix).extractors(extractors).build();
    }

}
