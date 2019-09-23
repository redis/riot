package com.redislabs.riot.cli.file;

import java.util.Locale;

import org.springframework.batch.item.file.FlatFileItemWriter;

import picocli.CommandLine.Option;

public class FileReaderOptions {

	@Option(required = true, names = { "-f",
			"--fields" }, arity = "1..*", description = "Fields to write to file", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = "--append", description = "Append to file if it exists")
	private boolean append;
	@Option(names = "--force-sync", description = "Force-sync changes to disk on flush")
	private boolean forceSync;
	@Option(names = "--line-sep", description = "String to separate lines (default: system default)", paramLabel = "<string>")
	private String lineSeparator = FlatFileItemWriter.DEFAULT_LINE_SEPARATOR;
	@Option(names = "--format", description = "Format string used to aggregate items", paramLabel = "<string>")
	private String format;
	@Option(names = "--locale", description = "Locale", paramLabel = "<tag>")
	private Locale locale = Locale.ENGLISH;
	@Option(names = "--max-length", description = "Max length of the formatted string", paramLabel = "<int>")
	private Integer maxLength;
	@Option(names = "--min-length", description = "Min length of the formatted string", paramLabel = "<int>")
	private Integer minLength;

	public String[] getNames() {
		return names;
	}

	public boolean isAppend() {
		return append;
	}

	public boolean isForceSync() {
		return forceSync;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public String getFormat() {
		return format;
	}

	public Locale getLocale() {
		return locale;
	}

	public Integer getMaxLength() {
		return maxLength;
	}

	public Integer getMinLength() {
		return minLength;
	}

}
