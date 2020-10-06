package com.redislabs.riot.file;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import lombok.Getter;
import picocli.CommandLine.Option;

@Getter
public class MapFileOptions {

	@Option(names = { "-t", "--filetype" }, description = "File type: ${COMPLETION-CANDIDATES}", paramLabel = "<type>")
	private FileType type;
	@Option(names = "--fields", arity = "1..*", description = "Delimited/FW field names", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = { "-h", "--header" }, description = "Delimited/FW first line contains field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter character", paramLabel = "<string>")
	private String delimiter;

	public String delimiter(String file) {
		if (delimiter == null) {
			String extension = FileOptions.extension(file);
			if (extension != null) {
				switch (extension) {
				case FileOptions.EXT_TSV:
					return DelimitedLineTokenizer.DELIMITER_TAB;
				case FileOptions.EXT_CSV:
					return DelimitedLineTokenizer.DELIMITER_COMMA;
				}
			}
			return DelimitedLineTokenizer.DELIMITER_COMMA;
		}
		return delimiter;
	}

}
