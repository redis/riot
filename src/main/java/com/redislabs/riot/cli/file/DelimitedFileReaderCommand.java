package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Slf4j
@Command(name = "csv", description = "Delimited file")
public class DelimitedFileReaderCommand extends AbstractFlatFileReaderCommand {

	@Option(names = "--fields", description = "Names of the fields in the order they occur within the file.", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = "--header", description = "Use first line to discover field names.")
	private boolean header = false;
	@Option(names = "--delimiter", description = "Delimiter used when reading input.", paramLabel = "<string>")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;
	@Option(names = "--quote", description = "Character to escape delimiters or line endings.", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
	@Option(names = "--include", description = "Fields to include in the output by position (starting at 0).", paramLabel = "<fields>")
	private Integer[] includedFields = new Integer[0];

	@Override
	protected int getLinesToSkip() {
		int linesToSkip = super.getLinesToSkip();
		if (header) {
			if (linesToSkip == 0) {
				return 1;
			}
		}
		return linesToSkip;
	}

	@Override
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = builder();
		builder.name("delimited-file-reader");
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		delimited.delimiter(delimiter);
		delimited.includedFields(includedFields);
		delimited.quoteCharacter(quoteCharacter);
		String[] fieldNames = Arrays.copyOf(names, names.length);
		if (header) {
			if (fieldNames.length == 0) {
				BufferedReader reader = new DefaultBufferedReaderFactory().create(resource(), getEncoding());
				DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
				tokenizer.setDelimiter(delimiter);
				tokenizer.setQuoteCharacter(quoteCharacter);
				if (includedFields.length > 0) {
					tokenizer.setIncludedFields(ArrayUtils.toPrimitive(includedFields));
				}
				fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
				log.debug("Found header {}", Arrays.asList(fieldNames));
			}
		}
		if (fieldNames == null || fieldNames.length == 0) {
			throw new IOException("No fields found");
		}
		delimited.names(fieldNames);
		return builder.build();
	}

}
