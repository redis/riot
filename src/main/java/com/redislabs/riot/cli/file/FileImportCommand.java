package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
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
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.riot.cli.ImportCommand;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

@Command(name = "file-import", description = "Import file into Redis")
public class FileImportCommand extends ImportCommand {

	private final Logger log = LoggerFactory.getLogger(FileImportCommand.class);

	@ArgGroup(multiplicity = "1")
	private PathGroup path = new PathGroup();
	@Mixin
	private FileOptions file = new FileOptions();
	@Option(names = { "-z", "--gzip" }, description = "File is gzip compressed")
	private boolean gzip;
	@Option(names = "--skip", description = "Lines to skip from the beginning of the file", paramLabel = "<count>")
	private Integer linesToSkip;
	@Option(names = "--include", arity = "1..*", description = "Indices of the fields within the delimited file to be included (0-based)", paramLabel = "<index>")
	private Integer[] includedFields = new Integer[0];
	@Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
	private Range[] columnRanges = new Range[0];

	static class PathGroup {
		@Option(names = { "-f", "--file" }, description = "File path")
		private File file;
		@Option(names = { "-u", "--url" }, description = "File URL")
		private URL url;

		public Resource resource() {
			if (file == null) {
				return new UrlResource(url);
			}
			return new FileSystemResource(file);
		}

	}

	private Resource resource() throws IOException {
		Resource resource = path.resource();
		if (gzip || resource.getFilename().toLowerCase().endsWith(".gz")) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileItemReaderBuilder() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.name("flat-file-reader");
		builder.resource(resource());
		if (file.getEncoding() != null) {
			builder.encoding(file.getEncoding());
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

	private FlatFileItemReader<Map<String, Object>> delimitedReader() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileItemReaderBuilder();
		if (file.isHeader() && linesToSkip == null) {
			builder.linesToSkip(1);
		}
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		delimitedBuilder.delimiter(file.getDelimiter());
		delimitedBuilder.includedFields(includedFields);
		delimitedBuilder.quoteCharacter(file.getQuoteCharacter());
		String[] fieldNames = Arrays.copyOf(file.getNames(), file.getNames().length);
		if (file.isHeader()) {
			BufferedReader reader = new DefaultBufferedReaderFactory().create(resource(), file.getEncoding());
			DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
			tokenizer.setDelimiter(file.getDelimiter());
			tokenizer.setQuoteCharacter(file.getQuoteCharacter());
			if (includedFields.length > 0) {
				int[] result = new int[includedFields.length];
				for (int i = 0; i < includedFields.length; i++) {
					result[i] = includedFields[i].intValue();
				}
				tokenizer.setIncludedFields(result);
			}
			fieldNames = tokenizer.tokenize(reader.readLine()).getValues();
			log.info("Found header {}", Arrays.asList(fieldNames));
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
		fixedlength.names(file.getNames());
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
		Resource resource = path.resource();
		Assert.isTrue(resource.exists(), String.format("%s does not exist", resource));
		switch (file.type(resource)) {
		case json:
			return jsonReader();
		case fixed:
			return fixedLengthReader();
		default:
			return delimitedReader();
		}
	}

	@Override
	protected String sourceDescription() {
		return String.valueOf(path.resource());
	}

}
