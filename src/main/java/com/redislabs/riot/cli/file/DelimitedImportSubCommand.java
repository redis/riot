package com.redislabs.riot.cli.file;

import java.io.IOException;
import java.util.Map;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.riot.cli.AbstractFlatFileImportSubCommand;
import com.redislabs.riot.file.FileReaderBuilder;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "csv", description = "Import a delimited file", sortOptions = false)
public class DelimitedImportSubCommand extends AbstractFlatFileImportSubCommand {

	@Option(names = "--header", description = "Use first line to discover field names.", order = 3)
	private boolean header = false;
	@Option(names = "--delimiter", description = "Delimiter used when reading input. (default: ${DEFAULT-VALUE}).", order = 4)
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;
	@Option(names = "--quote-character", description = "Character to escape delimiters or line endings. (default: ${DEFAULT-VALUE})", order = 5)
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
	@Option(arity = "1..*", names = "--included-fields", description = "Fields to include in the output by position (starting at 0). By default all fields are included, but this property can be set to pick out only a few fields from a larger set. Note that if field names are provided, their number must match the number of included fields.", order = 6)
	private int[] includedFields;

	@Override
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException {
		FileReaderBuilder builder = builder();
		builder.setDelimiter(delimiter);
		builder.setHeader(header);
		builder.setIncludedFields(includedFields);
		builder.setQuoteCharacter(quoteCharacter);
		return builder.buildDelimited();
	}

}
