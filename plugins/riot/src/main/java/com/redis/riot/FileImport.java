package com.redis.riot;

import com.redis.riot.file.FileType;

import picocli.CommandLine.Option;

public class FileImport extends AbstractFileImport {

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	private FileTypeEnum fileType;

	@Override
	public FileType getFileType() {
		if (fileType == null) {
			return null;
		}
		switch (fileType) {
		case CSV:
			return FileType.DELIMITED;
		case FW:
			return FileType.FIXED_WIDTH;
		case JSON:
			return FileType.JSON;
		case JSONL:
			return FileType.JSONL;
		case XML:
			return FileType.XML;
		}
		return null;
	}

	public void setFileType(FileTypeEnum fileType) {
		this.fileType = fileType;
	}

}
