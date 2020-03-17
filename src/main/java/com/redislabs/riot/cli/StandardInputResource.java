package com.redislabs.riot.cli;

import java.io.IOException;
import java.io.InputStream;

import org.springframework.core.io.InputStreamResource;

public class StandardInputResource extends InputStreamResource {
	public StandardInputResource() {
		super(System.in, "stdin");
	}

	@Override
	public InputStream getInputStream() throws IOException, IllegalStateException {
		return System.in;
	}
}