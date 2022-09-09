package com.redis.riot.file;

import java.util.Optional;

import org.springframework.core.io.Resource;

import picocli.CommandLine.Option;

public class DumpFileOptions extends FileOptions {

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}.", paramLabel = "<type>")
	protected Optional<DumpFileType> type = Optional.empty();

	public Optional<DumpFileType> getType() {
		return type;
	}

	public void setType(DumpFileType type) {
		this.type = Optional.of(type);
	}

	@Override
	public String toString() {
		return "DumpFileOptions [type=" + type + ", encoding=" + encoding + ", gzip=" + gzip + ", s3=" + s3 + ", gcs="
				+ gcs + "]";
	}

	public DumpFileType type(Resource resource) {
		if (type.isPresent()) {
			return type.get();
		}
		Optional<FileExtension> extension = FileUtils.extension(resource);
		return type(extension.orElseThrow(() -> new UnknownFileTypeException("Unknown file extension")));
	}

	private DumpFileType type(FileExtension extension) {
		switch (extension) {
		case XML:
			return DumpFileType.XML;
		case JSON:
			return DumpFileType.JSON;
		default:
			throw new UnsupportedOperationException("Unsupported file extension: " + extension);
		}
	}

}
