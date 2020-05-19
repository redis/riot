package com.redislabs.riot.convert.map;

import com.redislabs.riot.convert.field.MapToArrayConverter;
import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.EvalshaArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

@Builder
public class MapToEvalshaConverter<K, V> implements Converter<Map<K, V>, EvalshaArgs<K, V>> {

    private final Converter<Map<K, V>, K[]> keysConverter;
    private final Converter<Map<K, V>, V[]> argsConverter;

    @Builder(builderMethodName = "builder")
    private static MapToEvalshaConverter<String, String> builder(String[] keys, String[] args) {
        Converter<Map<String,String>, String[]> keysConverter = MapToArrayConverter.builder().fields(keys).build();
        Converter<Map<String,String>, String[]> argsConverter = MapToArrayConverter.builder().fields(args).build();
        return new MapToEvalshaConverter<String, String>(keysConverter, argsConverter);
    }

    public MapToEvalshaConverter(Converter<Map<K, V>, K[]> keysConverter, Converter<Map<K, V>, V[]> argsConverter) {
        this.keysConverter = keysConverter;
        this.argsConverter = argsConverter;
    }

    @Override
    public EvalshaArgs<K, V> convert(Map<K, V> source) {
        return new EvalshaArgs<>(keysConverter.convert(source), argsConverter.convert(source));
    }
}