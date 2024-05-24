package com.redis.riot.file;

public class SystemOutResource extends OutputStreamResource {

	public SystemOutResource() {
		super(System.out, "stdout", "Standard Output");
	}

}
