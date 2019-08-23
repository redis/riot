package com.redislabs.riot.cli.file;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;

import picocli.CommandLine.Option;

public class FileOptions {

	public static enum FileType {
		json, csv, fixed
	}

	@Option(names = { "-t", "--type" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@Option(names = { "-e",
			"--encoding" }, description = "File encoding (default: ${DEFAULT-VALUE})", paramLabel = "<charset>")
	private String encoding = FlatFileItemWriter.DEFAULT_CHARSET;
	@Option(names = { "-h", "--header" }, description = "First line contains field names")
	private boolean header;
	@Option(names = { "-f",
			"--fields" }, arity = "1..*", description = "Names of the fields as they occur in the file", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = { "-d",
			"--delimiter" }, description = "Delimiter character (default: ${DEFAULT-VALUE})", paramLabel = "<string>")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;
	@Option(names = { "-q",
			"--quote" }, description = "Character to escape delimiters or line endings (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	public String getEncoding() {
		return encoding;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public Character getQuoteCharacter() {
		return quoteCharacter;
	}

	public boolean isHeader() {
		return header;
	}

	public String[] getNames() {
		return names;
	}

	public FileType type(Resource resource) {
		if (type == null) {
			if (resource.getFilename().toLowerCase().endsWith(".json")) {
				return FileType.json;
			}
			return FileType.csv;
		}
		return type;
	}

}
