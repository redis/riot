package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.zip.GZIPInputStream;

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
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.riot.cli.ImportCommand;
import com.redislabs.riot.cli.redis.RedisConnectionOptions;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;

@Command(name = "import", description = "Import file")
public class FileImportCommand extends ImportCommand {

	private final Logger log = LoggerFactory.getLogger(FileImportCommand.class);

	@ParentCommand
	private FileConnector connector;
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@Option(names = { "--skip" }, description = "Lines to skip from the beginning of the file", paramLabel = "<count>")
	private Integer linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Indices of the fields within the delimited file to be included (0-based)", paramLabel = "<index>")
	private Integer[] includedFields = new Integer[0];
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private Range[] columnRanges = new Range[0];
	@Option(names = { "-q",
			"--quote" }, description = "Character to escape delimiters or line endings (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
	private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

	private Resource resource() throws IOException {
		Resource resource = connector.resource();
		if (gzip || resource.getFilename().toLowerCase().endsWith(".gz")) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileItemReaderBuilder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(resource());
		if (connector.getEncoding() != null) {
			builder.encoding(connector.getEncoding());
		}
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
		if (connector.isHeader() && linesToSkip == null) {
			builder.linesToSkip(1);
		}
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter(connector.getDelimiter());
		delimitedBuilder.includedFields(includedFields);
		delimitedBuilder.quoteCharacter(quoteCharacter);
		String[] fieldNames = Arrays.copyOf(connector.getNames(), connector.getNames().length);
		if (connector.isHeader()) {
			BufferedReader reader = new DefaultBufferedReaderFactory().create(resource(), connector.getEncoding());
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(connector.getDelimiter());
			tokenizer.setQuoteCharacter(quoteCharacter);
			if (includedFields.length > 0) {
				int[] result = new int[includedFields.length];
				for (int i = 0; i < includedFields.length; i++) {
					result[i] = includedFields[i].intValue();
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
		Assert.notEmpty(columnRanges, "Column ranges are required");
		fixedlength.columns(columnRanges);
		fixedlength.names(connector.getNames());
		return builder.build();
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private AbstractItemCountingItemStreamItemReader<Map<String, Object>> jsonReader() throws Exception {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<Map>();
		builder.name("json-file-reader");
		builder.resource(resource());
		JacksonJsonObjectReader<Map> objectReader = new JacksonJsonObjectReader<>(Map.class);
		objectReader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(objectReader);
		JsonItemReader<? extends Map> reader = builder.build();
		return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) reader;
	}

	@Override
	protected ItemReader<Map<String, Object>> reader() throws Exception {
		switch (connector.type()) {
		case json:
			return jsonReader();
		case fixed:
			return fixedLengthReader();
		default:
			return delimitedReader();
		}
	}

	@Override
	protected String name() {
		return "file-import";
	}

	@Override
	protected RedisConnectionOptions redis() {
		return connector.riot().redis();
	}

}
