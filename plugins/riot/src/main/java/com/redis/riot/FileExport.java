package com.redis.riot;

import org.springframework.util.MimeType;

import picocli.CommandLine.ArgGroup;

public class FileExport extends AbstractFileExport {

	@ArgGroup(exclusive = false)
	private FileTypeArgs fileTypeArgs = new FileTypeArgs();

	@Override
	public MimeType getFileType() {
		return fileTypeArgs.getType();
	}

	public FileTypeArgs getFileTypeArgs() {
		return fileTypeArgs;
	}

	public void setFileTypeArgs(FileTypeArgs fileTypeArgs) {
		this.fileTypeArgs = fileTypeArgs;
	}

}
