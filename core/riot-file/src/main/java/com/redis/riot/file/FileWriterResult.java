package com.redis.riot.file;

import org.springframework.batch.item.ItemWriter;
import org.springframework.core.io.Resource;
import org.springframework.util.MimeType;

public class FileWriterResult {

	private Resource resource;
	private MimeType type;
	private ItemWriter<?> writer;

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

	public ItemWriter<?> getWriter() {
		return writer;
	}

	public void setWriter(ItemWriter<?> writer) {
		this.writer = writer;
	}
}
