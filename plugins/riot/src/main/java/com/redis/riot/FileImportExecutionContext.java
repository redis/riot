package com.redis.riot;

public class FileImportExecutionContext extends RedisExecutionContext {

	private FileReaderFactory fileReaderFactory;

	public FileReaderFactory getFileReaderFactory() {
		return fileReaderFactory;
	}

	public void setFileReaderFactory(FileReaderFactory factory) {
		this.fileReaderFactory = factory;
	}

}
