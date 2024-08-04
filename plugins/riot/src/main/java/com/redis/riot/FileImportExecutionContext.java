package com.redis.riot;

import com.redis.riot.file.FileReaderFactory;

public class FileImportExecutionContext extends RedisExecutionContext {

	private FileReaderFactory fileReaderFactory;

	public FileReaderFactory getFileReaderFactory() {
		return fileReaderFactory;
	}

	public void setFileReaderFactory(FileReaderFactory factory) {
		this.fileReaderFactory = factory;
	}

}
