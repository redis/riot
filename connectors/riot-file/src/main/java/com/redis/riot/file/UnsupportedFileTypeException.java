package com.redis.riot.file;

import java.io.IOException;

@SuppressWarnings("serial")
public class UnsupportedFileTypeException extends IOException {

	private final FileType fileType;

	public UnsupportedFileTypeException(FileType fileType) {
		this.fileType = fileType;
	}

	public FileType getFileType() {
		return fileType;
	}

}
