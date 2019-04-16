package com.redislabs.riot.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.ArrayUtils;
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
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FileReaderBuilder {

	@Setter
	private File file;
	@Setter
	private URL url;
	@Setter
	private boolean gzip;
	@Setter
	private boolean header;
	@Setter
	private String delimiter;
	@Setter
	private int[] includedFields;
	@Setter
	private Character quoteCharacter;
	@Setter
	private String[] names = new String[0];
	@Setter
	private String encoding = FlatFileItemReader.DEFAULT_CHARSET;
	@Setter
	private Integer linesToSkip;
	@Setter
	private String[] columnRanges;

	private Resource resource(Resource resource) throws IOException {
		if (gzip) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	private Resource resource() throws IOException {
		if (url != null) {
			return resource(new UrlResource(url));
		}
		return resource(new FileSystemResource(file));
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFile(Resource resource) throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.resource(resource);
		if (encoding != null) {
			builder.encoding(encoding);
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		if (linesToSkip != null) {
			builder.linesToSkip(linesToSkip);
		}
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

	public FlatFileItemReader<Map<String, Object>> buildDelimited() throws IOException {
		Resource resource = resource();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFile(resource);
		builder.name("delimited-file-reader");
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		if (delimiter != null) {
			delimited.delimiter(delimiter);
		}
		if (includedFields != null) {
			delimited.includedFields(ArrayUtils.toObject(includedFields));
		}
		if (quoteCharacter != null) {
			delimited.quoteCharacter(quoteCharacter);
		}
		String[] fieldNames = Arrays.copyOf(names, names.length);
		if (header) {
			if (fieldNames.length == 0) {
				BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, encoding);
				DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
				if (delimiter != null) {
					tokenizer.setDelimiter(delimiter);
				}
				if (quoteCharacter != null) {
					tokenizer.setQuoteCharacter(quoteCharacter);
				}
				if (includedFields != null) {
					tokenizer.setIncludedFields(includedFields);
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

	public FlatFileItemReader<Map<String, Object>> buildFixedLength() throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFile(resource());
		builder.name("fixed-length-file-reader");
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

	@SuppressWarnings("rawtypes")
	public JsonItemReader<? extends Map> buildJson() throws IOException {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.name("json-file-reader");
		builder.resource(resource());
		JacksonJsonObjectReader<Map> reader = new JacksonJsonObjectReader<>(Map.class);
		reader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(reader);
		return builder.build();
	}

}
