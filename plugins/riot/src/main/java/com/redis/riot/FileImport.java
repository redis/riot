package com.redis.riot;

import org.springframework.util.MimeType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import data from files.")
public class FileImport extends AbstractFileImport {

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
