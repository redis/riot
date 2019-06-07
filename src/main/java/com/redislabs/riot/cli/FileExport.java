package com.redislabs.riot.cli;

import java.io.File;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import lombok.Getter;
import picocli.CommandLine.Option;

public abstract class FileExport extends ExportSub {

	@Option(names = "--file", required = true, description = "Path to output file.")
	private File file;
	@Getter
	@Option(names = "--append", description = "Append to the file if it already exists.")
	private boolean append;
	@Getter
	@Option(names = "--encoding", description = "Encoding for the output file. (default: ${DEFAULT-VALUE}).")
	private String encoding = FlatFileItemWriter.DEFAULT_CHARSET;
	@Getter
	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush.")
	private boolean forceSync;

	protected Resource resource() {
		return new FileSystemResource(file);
	}

}
