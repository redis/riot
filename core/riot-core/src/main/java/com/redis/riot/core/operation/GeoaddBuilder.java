package com.redis.riot.core.operation;

import java.util.Map;
import java.util.function.ToDoubleFunction;

import com.redis.spring.batch.common.ToGeoValueFunction;
import com.redis.spring.batch.writer.operation.Geoadd;

public class GeoaddBuilder extends AbstractCollectionMapOperationBuilder {

    private String longitude;

    private String latitude;

    public void setLongitude(String longitude) {
        this.longitude = longitude;
    }

    public void setLatitude(String latitude) {
        this.latitude = latitude;
    }

    @Override
    protected Geoadd<String, String, Map<String, Object>> operation() {
        Geoadd<String, String, Map<String, Object>> operation = new Geoadd<>();
        operation.setValueFunction(geoValue());
        return operation;
    }

    private ToGeoValueFunction<String, Map<String, Object>> geoValue() {
        ToDoubleFunction<Map<String, Object>> lon = toDouble(longitude, 0);
        ToDoubleFunction<Map<String, Object>> lat = toDouble(latitude, 0);
        return new ToGeoValueFunction<>(member(), lon, lat);
    }

}
