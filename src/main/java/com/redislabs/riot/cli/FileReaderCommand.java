package com.redislabs.riot.cli;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "file", description = "File")
public class FileReaderCommand extends AbstractReaderCommand {

	private final static Logger log = LoggerFactory.getLogger(FileReaderCommand.class);

	@Mixin
	private FileOptions input = new FileOptions();
	@Option(names = "--encoding", description = "File encoding", paramLabel = "<charset>")
	String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Option(names = "--skip", description = "Lines to skip from the beginning of the file", paramLabel = "<count>")
	int linesToSkip = 0;
	@Option(names = "--fields", arity = "1..*", description = "Names of the fields as they occur in the file", paramLabel = "<names>")
	private String[] names = new String[0];
	@Option(names = "--header", description = "Use first line to discover field names")
	private boolean header;
	@Option(names = "--delimiter", description = "Delimiter used when reading input", paramLabel = "<string>")
	private String delimiter = DelimitedLineTokenizer.DELIMITER_COMMA;
	@Option(names = "--quote", description = "Character to escape delimiters or line endings", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;
	@Option(names = "--include", arity = "1..*", description = "0-based indices of fields to include in the output", paramLabel = "<fields>")
	private Integer[] includedFields = new Integer[0];
	@Option(names = "--ranges", arity = "1..*", description = "Column ranges", paramLabel = "<integer>")
	private String[] columnRanges;

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileItemReaderBuilder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(input.resource());
		if (encoding != null) {
			builder.encoding(encoding);
		}
		builder.linesToSkip(linesToSkip());
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		return builder;
	}

	protected int linesToSkip() {
		if (header) {
			if (linesToSkip == 0) {
				return 1;
			}
		}
		return linesToSkip;
	}

	private static class MapFieldSetMapper implements FieldSetMapper<Map<String, Object>> {

		@Override
		public Map<String, Object> mapFieldSet(FieldSet fieldSet) {
			Map<String, Object> fields = new HashMap<>();
			String[] names = fieldSet.getNames();
			for (int index = 0; index < names.length; index++) {
				String name = names[index];
				String value = fieldSet.readString(index);
				if (value == null || value.length() == 0) {
					continue;
				}
				fields.put(name, value);
			}
			return fields;
		}
	}

	@Override
	protected AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() throws Exception {
		switch (input.fileType()) {
		case json:
			return jsonReader();
		case fixed:
			return fixedLengthReader();
		default:
			return delimitedReader();
		}
	}

	private FlatFileItemReader<Map<String, Object>> delimitedReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		delimited.delimiter(delimiter);
		delimited.includedFields(includedFields);
		delimited.quoteCharacter(quoteCharacter);
		String[] fieldNames = Arrays.copyOf(names, names.length);
		if (header) {
			if (fieldNames.length == 0) {
				BufferedReader reader = new DefaultBufferedReaderFactory().create(input.resource(), encoding);
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

	@Override
	protected String description() {
		return input.description();
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> fixedLengthReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		FixedLengthBuilder<Map<String, Object>> fixedlength = builder.fixedLength();
		Assert.notNull(columnRanges, "Column ranges are required");
		Range[] ranges = new Range[columnRanges.length];
		for (int index = 0; index < columnRanges.length; index++) {
			String[] split = columnRanges[index].split("-");
			ranges[index] = new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
		}
		fixedlength.columns(ranges);
		fixedlength.names(names);
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> jsonReader() throws Exception {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<Map>();
		builder.name("json-file-reader");
		builder.resource(input.resource());
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<? extends Map> reader = builder.build();
		return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader;
	}

}
