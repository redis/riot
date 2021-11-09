package com.redis.riot.file;

import picocli.CommandLine;

public class DumpFileImportOptions extends FileOptions {

	@CommandLine.Option(names = { "-t",
			"--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private DumpFileType type;

	public DumpFileType getType() {
		return type;
	}

	public void setType(DumpFileType type) {
		this.type = type;
	}

}
