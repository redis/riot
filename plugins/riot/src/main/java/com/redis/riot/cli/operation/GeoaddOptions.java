package com.redis.riot.cli.operation;

import picocli.CommandLine.Option;

public class GeoaddOptions {

	@Option(names = "--lon", description = "Longitude field.", paramLabel = "<field>")
	private String longitude;

	@Option(names = "--lat", description = "Latitude field.", paramLabel = "<field>")
	private String latitude;

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitudeField) {
		this.longitude = longitudeField;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitudeField) {
		this.latitude = latitudeField;
	}

	@Override
	public String toString() {
		return "GeoaddOptions [longitude=" + longitude + ", latitude=" + latitude + "]";
	}

}
