package com.redis.riot.redis;

import io.lettuce.core.GeoValue;
import org.springframework.batch.item.redis.support.RedisOperation;
import org.springframework.batch.item.redis.support.convert.KeyMaker;
import org.springframework.batch.item.redis.support.operation.Geoadd;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Map;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractCollectionCommand {

    @SuppressWarnings("unused")
    @Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
    private String longitudeField;
    @SuppressWarnings("unused")
    @Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
    private String latitudeField;

    @Override
    public RedisOperation<String, String, Map<String, Object>> operation() {
        return Geoadd.key(key()).value(new GeoValueConverter(member(), doubleFieldExtractor(longitudeField), doubleFieldExtractor(latitudeField))).build();
    }

    private static class GeoValueConverter implements Converter<Map<String, Object>, GeoValue<String>> {

        private Converter<Map<String, Object>, String> member;
        private Converter<Map<String, Object>, Double> longitude;
        private Converter<Map<String, Object>, Double> latitude;

        public GeoValueConverter(KeyMaker<Map<String, Object>> member, Converter<Map<String, Object>, Double> longitude, Converter<Map<String, Object>, Double> latitude) {
            this.member = member;
            this.longitude = longitude;
            this.latitude = latitude;
        }

        @Override
        public GeoValue<String> convert(Map<String, Object> source) {
            Double longitude = this.longitude.convert(source);
            if (longitude == null) {
                return null;
            }
            Double latitude = this.latitude.convert(source);
            if (latitude == null) {
                return null;
            }
            return GeoValue.just(longitude, latitude, member.convert(source));
        }
    }

}
