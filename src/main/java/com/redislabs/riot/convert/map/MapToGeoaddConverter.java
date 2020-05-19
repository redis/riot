package com.redislabs.riot.convert.map;

import org.springframework.batch.item.redis.support.commands.GeoaddArgs;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public class MapToGeoaddConverter<K, V> extends AbstractMapToCollectionConverter<K, V, GeoaddArgs<K, V>> {

    private final Converter<Map<K, V>, Double> longitudeConverter;
    private final Converter<Map<K, V>, Double> latitudeConverter;

    protected MapToGeoaddConverter(Converter<Map<K, V>, K> keyConverter, Converter<Map<K, V>, V> memberIdConverter, Converter<Map<K, V>, Double> longitudeConverter, Converter<Map<K, V>, Double> latitudeConverter) {
        super(keyConverter, memberIdConverter);
        this.longitudeConverter = longitudeConverter;
        this.latitudeConverter = latitudeConverter;
    }

    @Override
    protected GeoaddArgs<K, V> convert(Map<K, V> source, K key, V memberId) {
        return new GeoaddArgs<>(key, memberId, longitudeConverter.convert(source), latitudeConverter.convert(source));
    }
}
