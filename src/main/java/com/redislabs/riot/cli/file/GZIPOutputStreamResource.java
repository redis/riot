package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import com.redislabs.riot.file.OutputStreamResource;

public class GZIPOutputStreamResource extends OutputStreamResource {

	public GZIPOutputStreamResource(OutputStream outStream) throws IOException {
		super(new GZIPOutputStream(outStream));
	}

	public GZIPOutputStreamResource(OutputStream outStream, String desc) throws IOException {
		super(new GZIPOutputStream(outStream), desc);
	}

}
