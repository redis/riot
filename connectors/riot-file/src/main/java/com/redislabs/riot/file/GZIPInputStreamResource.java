package com.redislabs.riot.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.springframework.core.io.InputStreamResource;

public class GZIPInputStreamResource extends InputStreamResource {

	public GZIPInputStreamResource(InputStream inputStream, String description) throws IOException {
		super(new GZIPInputStream(inputStream), description);
	}

}
