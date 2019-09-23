package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import file")
public class FileImportCommand extends ImportCommand {

	private final Logger log = LoggerFactory.getLogger(FileImportCommand.class);

	@ArgGroup(exclusive = false, heading = "File options%n", order = 2)
	private FileOptions fileOptions;
	@ArgGroup(exclusive = false, heading = "File writer options%n", order = 3)
	private FileWriterOptions writerOptions = new FileWriterOptions();

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileItemReaderBuilder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(fileOptions.inputResource());
		if (fileOptions.getEncoding() != null) {
			builder.encoding(fileOptions.getEncoding());
		}
		if (writerOptions.getLinesToSkip() != null) {
			builder.linesToSkip(writerOptions.getLinesToSkip());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		return builder;
	}

	private FlatFileItemReader<Map<String, Object>> delimitedReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		if (fileOptions.isHeader() && writerOptions.getLinesToSkip() == null) {
			builder.linesToSkip(1);
		}
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter(fileOptions.getDelimiter());
		delimitedBuilder.includedFields(writerOptions.getIncludedFields());
		delimitedBuilder.quoteCharacter(writerOptions.getQuoteCharacter());
		String[] fieldNames = Arrays.copyOf(writerOptions.getNames(), writerOptions.getNames().length);
		if (fileOptions.isHeader()) {
			BufferedReader reader = new DefaultBufferedReaderFactory().create(fileOptions.inputResource(),
					fileOptions.getEncoding());
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(fileOptions.getDelimiter());
			tokenizer.setQuoteCharacter(writerOptions.getQuoteCharacter());
			if (writerOptions.getIncludedFields().length > 0) {
				int[] result = new int[writerOptions.getIncludedFields().length];
				for (int i = 0; i < writerOptions.getIncludedFields().length; i++) {
					result[i] = writerOptions.getIncludedFields()[i].intValue();
				}
				tokenizer.setIncludedFields(result);
			}
			fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
			log.debug("Found header {}", Arrays.asList(fieldNames));
		}
		if (fieldNames == null || fieldNames.length == 0) {
			throw new IOException("No fields found");
		}
		delimitedBuilder.names(fieldNames);
		return builder.build();
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> fixedLengthReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		FixedLengthBuilder<Map<String, Object>> fixedlength = builder.fixedLength();
		Assert.notEmpty(writerOptions.getColumnRanges(), "Column ranges are required");
		fixedlength.columns(writerOptions.getColumnRanges());
		fixedlength.names(writerOptions.getNames());
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> jsonReader() throws Exception {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<Map>();
		builder.name("json-file-reader");
		builder.resource(fileOptions.inputResource());
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<? extends Map> reader = builder.build();
		return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader;
	}

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		switch (fileOptions.type()) {
		case json:
			return jsonReader();
		case fixed:
			return fixedLengthReader();
		default:
			return delimitedReader();
		}
	}

}
