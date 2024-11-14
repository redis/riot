package com.redis.riot.file;

import java.io.InputStream;

public class SystemInResource extends NamedInputStreamResource {

	public static final String FILENAME = "stdin";
	public static final String DESCRIPTION = "Standard Input";

	public SystemInResource() {
		this(System.in);
	}

	public SystemInResource(InputStream inputStream) {
		super(inputStream, FILENAME, DESCRIPTION);
	}

}
