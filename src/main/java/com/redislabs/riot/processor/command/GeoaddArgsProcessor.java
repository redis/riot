package com.redislabs.riot.processor.command;

import lombok.Builder;
import org.springframework.batch.item.redis.support.commands.GeoaddArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class GeoaddArgsProcessor<K, V> extends MemberArgsProcessor<K, V> {

    private final Converter<Map<K, V>, Double> longitudeConverter;
    private final Converter<Map<K, V>, Double> latitudeConverter;

    @Builder
    public GeoaddArgsProcessor(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter, Converter<Map<K, V>, Double> longitudeConverter, Converter<Map<K, V>, Double> latitudeConverter) {
        super(keyConverter, memberIdConverter);
        this.longitudeConverter = longitudeConverter;
        this.latitudeConverter = latitudeConverter;
    }

    @Override
    protected GeoaddArgs<K, V> process(K key, V memberId, Map<K, V> item) {
        Double longitude = longitudeConverter.convert(item);
        if (longitude == null) {
            return null;
        }
        Double latitude = latitudeConverter.convert(item);
        if (latitude == null) {
            return null;
        }
        return new GeoaddArgs<>(key, memberId, longitude, latitude);
    }

}
