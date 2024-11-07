package com.redis.riot.file;

public class SystemInResource extends FilenameInputStreamResource {

	private static final String FILENAME = "stdin";
	private static final String DESCRIPTION = "Standard Input";

	public SystemInResource() {
		super(System.in, FILENAME, DESCRIPTION);
	}

}
