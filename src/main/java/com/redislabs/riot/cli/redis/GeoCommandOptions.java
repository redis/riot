package com.redislabs.riot.cli.redis;

import com.redislabs.riot.batch.redis.map.GeoaddMapWriter;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class GeoCommandOptions {

	@Option(names = "--geo-lon", description = "Longitude field", paramLabel = "<field>")
	private String geoLon;
	@Option(names = "--geo-lat", description = "Latitude field", paramLabel = "<field>")
	private String geoLat;

	public <R> GeoaddMapWriter<R> writer() {
		return new GeoaddMapWriter<R>().latitudeField(geoLat).longitudeField(geoLon);
	}

}
