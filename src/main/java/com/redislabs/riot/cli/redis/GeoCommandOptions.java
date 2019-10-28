package com.redislabs.riot.cli.redis;

import com.redislabs.riot.batch.redis.writer.GeoaddMapWriter;
import com.redislabs.riot.batch.redis.writer.RedisMapWriter;

import lombok.Data;
import picocli.CommandLine.Option;

public @Data class GeoCommandOptions {

	@Option(names = "--geo-lon", description = "Longitude field", paramLabel = "<field>")
	private String geoLon;
	@Option(names = "--geo-lat", description = "Latitude field", paramLabel = "<field>")
	private String geoLat;

	public RedisMapWriter writer() {
		GeoaddMapWriter writer = new GeoaddMapWriter();
		writer.setLatitudeField(geoLat);
		writer.setLongitudeField(geoLon);
		return writer;
	}

}
