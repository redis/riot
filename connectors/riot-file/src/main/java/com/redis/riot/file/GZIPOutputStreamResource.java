package com.redis.riot.file;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.springframework.batch.item.resource.OutputStreamResource;

public class GZIPOutputStreamResource extends OutputStreamResource {

	public GZIPOutputStreamResource(OutputStream outStream, String desc) throws IOException {
		super(new GZIPOutputStream(outStream), desc);
	}

}
