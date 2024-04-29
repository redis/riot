package com.redis.riot.file;

import java.io.InputStream;
import java.util.Objects;

import org.springframework.core.io.InputStreamResource;

import com.redis.riot.file.FilenameInputStreamResource;

public class FilenameInputStreamResource extends InputStreamResource {

	private final String filename;

	public FilenameInputStreamResource(InputStream inputStream, String filename, String description) {
		super(inputStream, description);
		this.filename = filename;
	}

	@Override
	public String getFilename() {
		return filename;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + Objects.hash(filename);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		FilenameInputStreamResource other = (FilenameInputStreamResource) obj;
		return Objects.equals(filename, other.filename);
	}

}
