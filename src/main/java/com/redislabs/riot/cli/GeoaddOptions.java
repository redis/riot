package com.redislabs.riot.cli;

import com.redislabs.riot.redis.writer.map.Geoadd;

import picocli.CommandLine.Option;

public class GeoaddOptions {

	@Option(names = "--lon", description = "Longitude field", paramLabel = "<field>")
	private String longitude;
	@Option(names = "--lat", description = "Latitude field", paramLabel = "<field>")
	private String latitude;

	public Geoadd geoadd() {
		return new Geoadd().longitude(longitude).latitude(latitude);
	}

}
