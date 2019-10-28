package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

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
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "file-import", description = "Import file")
@Slf4j
public class FileImportCommand extends ImportCommand {

	@Setter
	@ArgGroup(exclusive = false, heading = "File writer options%n", order = 3)
	private FileReaderOptions fileReaderOptions = new FileReaderOptions();

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileItemReaderBuilder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(fileReaderOptions.inputResource());
		if (fileReaderOptions.getEncoding() != null) {
			builder.encoding(fileReaderOptions.getEncoding());
		}
		if (fileReaderOptions.getLinesToSkip() != null) {
			builder.linesToSkip(fileReaderOptions.getLinesToSkip());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		return builder;
	}

	private FlatFileItemReader<Map<String, Object>> delimitedReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		if (fileReaderOptions.isHeader() && fileReaderOptions.getLinesToSkip() == null) {
			builder.linesToSkip(1);
		}
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter(fileReaderOptions.getDelimiter());
		delimitedBuilder.includedFields(fileReaderOptions.getIncludedFields()
				.toArray(new Integer[fileReaderOptions.getIncludedFields().size()]));
		delimitedBuilder.quoteCharacter(fileReaderOptions.getQuoteCharacter());
		String[] fieldNames = fileReaderOptions.getNames().toArray(new String[fileReaderOptions.getNames().size()]);
		if (fileReaderOptions.isHeader()) {
			BufferedReader reader = new DefaultBufferedReaderFactory().create(fileReaderOptions.inputResource(),
					fileReaderOptions.getEncoding());
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(fileReaderOptions.getDelimiter());
			tokenizer.setQuoteCharacter(fileReaderOptions.getQuoteCharacter());
			if (!fileReaderOptions.getIncludedFields().isEmpty()) {
				int[] result = new int[fileReaderOptions.getIncludedFields().size()];
				for (int i = 0; i < fileReaderOptions.getIncludedFields().size(); i++) {
					result[i] = fileReaderOptions.getIncludedFields().get(i).intValue();
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
		Assert.notEmpty(fileReaderOptions.getColumnRanges(), "Column ranges are required");
		fixedlength.columns(
				fileReaderOptions.getColumnRanges().toArray(new Range[fileReaderOptions.getColumnRanges().size()]));
		fixedlength.names(fileReaderOptions.getNames().toArray(new String[fileReaderOptions.getNames().size()]));
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> jsonReader() throws Exception {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<Map>();
		builder.name("json-file-reader");
		builder.resource(fileReaderOptions.inputResource());
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<? extends Map> reader = builder.build();
		return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader;
	}

	@Override
	protected ItemReader<Map<String, Object>> reader(RedisConnectionOptions redisOptions) throws Exception {
		switch (fileReaderOptions.type()) {
		case json:
			return jsonReader();
		case fixed:
			return fixedLengthReader();
		default:
			return delimitedReader();
		}
	}

}
