package com.redis.riot.file;

import java.util.Optional;

import org.springframework.core.io.Resource;

import picocli.CommandLine.Option;

public class FileDumpOptions {

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	protected Optional<FileDumpType> type = Optional.empty();

	public Optional<FileDumpType> getType() {
		return type;
	}

	public void setType(FileDumpType type) {
		this.type = Optional.of(type);
	}

	@Override
	public String toString() {
		return "DumpFileOptions [type=" + type + "]";
	}

	public FileDumpType type(Resource resource) {
		if (type.isPresent()) {
			return type.get();
		}
		FileExtension extension = FileUtils.extension(resource);
		return type(extension);
	}

	private FileDumpType type(FileExtension extension) {
		switch (extension) {
		case XML:
			return FileDumpType.XML;
		case JSON:
			return FileDumpType.JSON;
		default:
			throw new UnsupportedOperationException("Unsupported file extension: " + extension);
		}
	}

}
