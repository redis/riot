package com.redis.riot.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

import org.springframework.core.io.AbstractResource;
import org.springframework.core.io.WritableResource;

public class OutputStreamResource extends AbstractResource implements WritableResource {

	private final OutputStream outStream;
	private final String filename;
	private final String desc;

	public OutputStreamResource(OutputStream outStream, String filename, String desc) {
		this.outStream = outStream;
		this.filename = filename;
		this.desc = desc;
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		return this.outStream;
	}

	@Override
	public String getDescription() {
		return this.desc;
	}

	@Override
	public String getFilename() {
		return filename;
	}

	@Override
	public InputStream getInputStream() throws IOException {
		throw new IOException("Unable to create input stream.");
	}

	@Override
	public boolean isWritable() {
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(desc, outStream);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

}
