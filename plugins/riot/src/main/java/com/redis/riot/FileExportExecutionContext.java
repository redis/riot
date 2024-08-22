package com.redis.riot;

public class FileExportExecutionContext extends RedisExecutionContext {

	private FileWriterFactory fileWriterFactory;

	public FileWriterFactory getFileWriterFactory() {
		return fileWriterFactory;
	}

	public void setFileWriterFactory(FileWriterFactory factory) {
		this.fileWriterFactory = factory;
	}

}
