package com.redis.riot.file;

import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;

public class FileReaderResult {

	private Resource resource;
	private MimeType type;
	private ItemReader<?> reader;

	public Resource getResource() {
		return resource;
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public MimeType getType() {
		return type;
	}

	public void setType(MimeType mimeType) {
		this.type = mimeType;
	}

	public ItemReader<?> getReader() {
		return reader;
	}

	public void setReader(ItemReader<?> itemReader) {
		this.reader = itemReader;
	}

}
