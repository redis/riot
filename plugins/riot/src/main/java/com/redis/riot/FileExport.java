package com.redis.riot;

import org.springframework.util.MimeType;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-export", description = "Export Redis data to files.")
public class FileExport extends AbstractFileExport {

	@ArgGroup(exclusive = false)
	private FileTypeArgs fileTypeArgs = new FileTypeArgs();

	@Override
	protected MimeType getFileType() {
		return fileTypeArgs.getType();
	}

	public FileTypeArgs getFileTypeArgs() {
		return fileTypeArgs;
	}

	public void setFileTypeArgs(FileTypeArgs fileTypeArgs) {
		this.fileTypeArgs = fileTypeArgs;
	}

}
