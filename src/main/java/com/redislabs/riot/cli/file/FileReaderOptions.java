package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.riot.batch.MapFlattener;

import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@EqualsAndHashCode(callSuper = true)
@Slf4j
public class FileReaderOptions extends FlatFileOptions {

	@Option(names = { "--skip" }, description = "Lines to skip from the beginning of the file", paramLabel = "<count>")
	private Integer linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Field indices to include (0-based)", paramLabel = "<index>")
	private List<Integer> includedFields = new ArrayList<>();
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private List<Range> columnRanges = new ArrayList<>();
	@Option(names = { "-q",
			"--quote" }, description = "Escape character (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileItemReaderBuilder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(inputResource());
		builder.encoding(encoding());
		if (linesToSkip != null) {
			builder.linesToSkip(linesToSkip);
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		return builder;
	}

	private FlatFileItemReader<Map<String, Object>> delimitedReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		if (header() && linesToSkip == null) {
			builder.linesToSkip(1);
		}
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter(delimiter());
		delimitedBuilder.includedFields(includedFields.toArray(new Integer[includedFields.size()]));
		delimitedBuilder.quoteCharacter(quoteCharacter);
		String[] fieldNames = names();
		if (header()) {
			BufferedReader reader = new DefaultBufferedReaderFactory().create(inputResource(), encoding());
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(delimiter());
			tokenizer.setQuoteCharacter(quoteCharacter);
			if (!includedFields.isEmpty()) {
				int[] result = new int[includedFields.size()];
				for (int i = 0; i < includedFields.size(); i++) {
					result[i] = includedFields.get(i).intValue();
				}
				tokenizer.setIncludedFields(result);
			}
			fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
			log.debug("Found header {}", Arrays.asList(fieldNames));
		}
		if (fieldNames == null || fieldNames.length == 0) {
			throw new IOException("No fields specified");
		}
		delimitedBuilder.names(fieldNames);
		return builder.build();
	}

	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> fixedLengthReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		FixedLengthBuilder<Map<String, Object>> fixedlength = builder.fixedLength();
		Assert.notEmpty(columnRanges, "Column ranges are required");
		fixedlength.columns(columnRanges.toArray(new Range[columnRanges.size()]));
		fixedlength.names(names());
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> jsonReader() throws Exception {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<Map>();
		builder.name("json-file-reader");
		builder.resource(inputResource());
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<? extends Map> reader = builder.build();
		return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader;
	}

	public ItemReader<Map<String, Object>> reader() throws Exception {
		switch (type()) {
		case Json:
			return jsonReader();
		case Fixed:
			return fixedLengthReader();
		default:
			return delimitedReader();
		}
	}

	public ItemProcessor<Map<String, Object>, Map<String, Object>> postProcessor() {
		switch (type()) {
		case Json:
			return new MapFlattener();
		default:
			return null;
		}
	}

}
