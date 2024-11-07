package com.redis.riot.file;

public class SystemOutResource extends OutputStreamResource {

	public static final String FILENAME = "stdout";
	public static final String DESCRIPTION = "Standard Output";

	public SystemOutResource() {
		super(System.out, FILENAME, DESCRIPTION);
	}

}
