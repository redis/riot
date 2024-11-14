package com.redis.riot.file;

import java.io.OutputStream;

public class SystemOutResource extends OutputStreamResource {

	public static final String FILENAME = "stdout";
	public static final String DESCRIPTION = "Standard Output";

	public SystemOutResource() {
		this(System.out);
	}

	public SystemOutResource(OutputStream outputStream) {
		super(outputStream, FILENAME, DESCRIPTION);
	}

}
