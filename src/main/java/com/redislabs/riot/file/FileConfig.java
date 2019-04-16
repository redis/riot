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
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class FileConfig {

	public Resource gzip(Resource resource) throws IOException {
		return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
	}

	public Resource resource(URL url) {
		return new UrlResource(url);
	}

	public Resource resource(File file) {
		return new FileSystemResource(file);
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFile(Resource resource, FlatFileOptions options)
			throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.resource(resource);
		if (options.getEncoding() != null) {
			builder.encoding(options.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.linesToSkip(options.getLinesToSkip());
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

	public FlatFileItemReader<Map<String, Object>> reader(Resource resource, DelimitedFileOptions options)
			throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFile(resource, options);
		builder.name("delimited-file-reader");
		DelimitedBuilder<Map<String, Object>> delimited = builder.delimited();
		if (options.getDelimiter() != null) {
			delimited.delimiter(options.getDelimiter());
		}
		if (options.getIncludedFields() != null) {
			delimited.includedFields(ArrayUtils.toObject(options.getIncludedFields()));
		}
		if (options.getQuoteCharacter() != null) {
			delimited.quoteCharacter(options.getQuoteCharacter());
		}
		String[] names = options.getNames();
		if (options.isHeader()) {
			if (options.getNames() == null) {
				try {
					BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, options.getEncoding());
					DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
					if (options.getDelimiter() != null) {
						tokenizer.setDelimiter(options.getDelimiter());
					}
					if (options.getQuoteCharacter() != null) {
						tokenizer.setQuoteCharacter(options.getQuoteCharacter());
					}
					if (options.getIncludedFields() != null) {
						tokenizer.setIncludedFields(options.getIncludedFields());
					}
					String line = reader.readLine();
					names = tokenizer.tokenize(line).getValues();
					log.info("Found header {}", Arrays.asList(names));
				} catch (Exception e) {
					log.error("Could not read header for file {}", resource, e);
				}
			}
		}
		if (names == null || names.length == 0) {
			throw new IOException("No fields found");
		}
		delimited.names(names);
		return builder.build();
	}

	public FlatFileItemReader<Map<String, Object>> reader(Resource resource, FixedLengthFileOptions options)
			throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFile(resource, options);
		builder.name("fixed-length-file-reader");
		FixedLengthBuilder<Map<String, Object>> fixedlength = builder.fixedLength();
		if (options.getRanges() != null) {
			fixedlength.columns(ranges(options.getRanges()));
		}
		if (options.getNames() != null) {
			fixedlength.names(options.getNames());
		}
		return builder.build();
	}

	private Range[] ranges(String[] strings) {
		Range[] ranges = new Range[strings.length];
		for (int index = 0; index < strings.length; index++) {
			ranges[index] = range(strings[index]);
		}
		return ranges;
	}

	private Range range(String string) {
		String[] split = string.split("-");
		return new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

	@SuppressWarnings("rawtypes")
	public JsonItemReader<? extends Map> reader(Resource resource) throws IOException {
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.name("json-file-reader");
		builder.resource(resource);
		JacksonJsonObjectReader<Map> reader = new JacksonJsonObjectReader<>(Map.class);
		reader.setMapper(new ObjectMapper());
		builder.jsonObjectReader(reader);
		return builder.build();
	}

}
