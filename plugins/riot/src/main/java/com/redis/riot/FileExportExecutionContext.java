package com.redis.riot;

import com.redis.riot.file.FileWriterFactory;

public class FileExportExecutionContext extends RedisExecutionContext {

	private FileWriterFactory fileWriterFactory;

	public FileWriterFactory getFileWriterFactory() {
		return fileWriterFactory;
	}

	public void setFileWriterFactory(FileWriterFactory factory) {
		this.fileWriterFactory = factory;
	}

}
