package com.redis.riot.file;

import java.util.Optional;

import picocli.CommandLine;

public class DumpFileImportOptions extends FileOptions {

	@CommandLine.Option(names = { "-t",
			"--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private Optional<DumpFileType> type = Optional.empty();

	public Optional<DumpFileType> getType() {
		return type;
	}

	public void setType(DumpFileType type) {
		this.type = Optional.of(type);
	}

}
