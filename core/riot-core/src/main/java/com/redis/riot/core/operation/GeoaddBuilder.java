package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.ToDoubleFunction;

import com.redis.spring.batch.util.ToGeoValueFunction;
import com.redis.spring.batch.writer.operation.Geoadd;

public class GeoaddBuilder extends AbstractCollectionOperationBuilder<GeoaddBuilder> {

    private String longitude;

    private String latitude;

    public GeoaddBuilder latitude(String field) {
        this.latitude = field;
        return this;
    }

    public GeoaddBuilder longitude(String field) {
        this.longitude = field;
        return this;
    }

    @Override
    protected Geoadd<String, String, Map<String, Object>> operation() {
        return new Geoadd<String, String, Map<String, Object>>().value(geoValue());
    }

    private ToGeoValueFunction<String, Map<String, Object>> geoValue() {
        ToDoubleFunction<Map<String, Object>> lon = toDouble(longitude, 0);
        ToDoubleFunction<Map<String, Object>> lat = toDouble(latitude, 0);
        return new ToGeoValueFunction<>(member(), lon, lat);
    }

}
