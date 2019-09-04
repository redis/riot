package com.redislabs.riot.cli.file;

import java.net.MalformedURLException;
import java.net.URI;

import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import com.redislabs.riot.Riot;
import com.redislabs.riot.cli.HelpAwareCommand;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(name = "file", description = "File import/export", subcommands = { FileImportCommand.class,
		FileExportCommand.class })
public class FileConnector extends HelpAwareCommand {

	@ParentCommand
	private Riot riot;
	@Parameters(arity = "1", description = "File path")
	private String file;
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

	public String getEncoding() {
		return encoding;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public boolean isHeader() {
		return header;
	}

	public String[] getNames() {
		return names;
	}

	public FileType type() {
		if (type == null) {
			if (file.toLowerCase().endsWith(".json") || file.toLowerCase().endsWith(".json.gz")) {
				return FileType.json;
			}
			return FileType.csv;
		}
		return type;
	}

	public Resource resource() throws MalformedURLException {
		URI uri = URI.create(file);
		if (uri.isAbsolute()) {
			return new UrlResource(uri);
		}
		return new FileSystemResource(file);
	}

	public Riot riot() {
		return riot;
	}

}
