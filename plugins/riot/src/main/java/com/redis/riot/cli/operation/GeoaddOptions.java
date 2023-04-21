package com.redis.riot.cli.operation;

import picocli.CommandLine.Option;

public class GeoaddOptions {

	@Option(names = "--lon", description = "Longitude field.", paramLabel = "<field>")
	private String longitudeField;
	@Option(names = "--lat", description = "Latitude field.", paramLabel = "<field>")
	private String latitudeField;

	public String getLongitudeField() {
		return longitudeField;
	}

	public void setLongitudeField(String longitudeField) {
		this.longitudeField = longitudeField;
	}

	public String getLatitudeField() {
		return latitudeField;
	}

	public void setLatitudeField(String latitudeField) {
		this.latitudeField = latitudeField;
	}

	@Override
	public String toString() {
		return "GeoaddOptions [longitudeField=" + longitudeField + ", latitudeField=" + latitudeField + "]";
	}

}
