package com.redis.riot.operation;

import java.util.Map;
import java.util.function.ToDoubleFunction;

import com.redis.riot.function.ToGeoValue;
import com.redis.spring.batch.item.redis.writer.impl.Geoadd;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "geoadd", description = "Add members to a geo set")
public class GeoaddCommand extends AbstractMemberOperationCommand {

	@Option(names = "--lon", required = true, description = "Longitude field.", paramLabel = "<field>")
	private String longitude;

	@Option(names = "--lat", required = true, description = "Latitude field.", paramLabel = "<field>")
	private String latitude;

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String field) {
		this.longitude = field;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String field) {
		this.latitude = field;
	}

	@Override
	public Geoadd<String, String, Map<String, Object>> operation() {
		return new Geoadd<>(keyFunction(), geoValueFunction());
	}

	private ToGeoValue<String, Map<String, Object>> geoValueFunction() {
		ToDoubleFunction<Map<String, Object>> lon = toDouble(longitude, 0);
		ToDoubleFunction<Map<String, Object>> lat = toDouble(latitude, 0);
		return new ToGeoValue<>(memberFunction(), lon, lat);
	}

}