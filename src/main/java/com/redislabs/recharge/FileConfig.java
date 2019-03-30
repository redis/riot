package com.redislabs.recharge;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.ArrayUtils;
import org.springframework.batch.core.configuration.annotation.StepScope;
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
import org.springframework.batch.item.json.JsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.recharge.FileProperties.FileType;

import lombok.extern.slf4j.Slf4j;

@Configuration
@EnableConfigurationProperties(FileProperties.class)
@Slf4j
public class FileConfig {

	@Bean
	@ConditionalOnProperty("file.path")
	public Resource resource(FileProperties file) throws IOException {
		Resource resource = resource(file.getPath());
		if (isGzip(file)) {
			return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
		}
		return resource;
	}

	private boolean isGzip(FileProperties file) {
		if (file.getGzip() == null) {
			return file.getPath().endsWith(".gz");
		}
		return file.getGzip();
	}

	private Resource resource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private FlatFileItemReaderBuilder<Map<String, Object>> flatFileBuilder(Resource resource, FileProperties flatFile)
			throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
		builder.resource(resource);
		if (flatFile.getEncoding() != null) {
			builder.encoding(flatFile.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.linesToSkip(flatFile.getLinesToSkip());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		if (flatFile.isHeader() && flatFile.getLinesToSkip() == 0) {
			builder.linesToSkip(1);
		}
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

	@SuppressWarnings("unchecked")
	@Bean
	@StepScope
	@ConditionalOnProperty("file.path")
	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> fileReader(Resource resource,
			FileProperties props) throws IOException, RechargeException {
		if (props.getType() != null) {
			if (props.getType() == FileType.Fw) {
				return fixedLengthReader(resource, props);
			}
			if (props.getType() == FileType.Json) {
				return (AbstractItemCountingItemStreamItemReader<Map<String, Object>>) jsonReader(resource, props);
			}
		}
		return delimitedReader(resource, props);
	}

	private FlatFileItemReader<Map<String, Object>> delimitedReader(Resource resource, FileProperties props)
			throws IOException, RechargeException {
		log.info("Reading delimited file {}", props);
		FlatFileItemReaderBuilder<Map<String, Object>> fileBuilder = flatFileBuilder(resource, props);
		fileBuilder.name("delimited-file-reader");
		DelimitedBuilder<Map<String, Object>> builder = fileBuilder.delimited();
		if (props.getDelimiter() != null) {
			builder.delimiter(props.getDelimiter());
		}
		if (props.getIncludedFields() != null) {
			builder.includedFields(ArrayUtils.toObject(props.getIncludedFields()));
		}
		if (props.getQuoteCharacter() != null) {
			builder.quoteCharacter(props.getQuoteCharacter());
		}
		String[] fields = props.getFields();
		if (props.isHeader()) {
			fileBuilder.linesToSkip(1);
			if (props.getFields() == null) {
				try {
					BufferedReader reader = new DefaultBufferedReaderFactory().create(resource, props.getEncoding());
					DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
					if (props.getDelimiter() != null) {
						tokenizer.setDelimiter(props.getDelimiter());
					}
					if (props.getQuoteCharacter() != null) {
						tokenizer.setQuoteCharacter(props.getQuoteCharacter());
					}
					if (props.getIncludedFields() != null) {
						tokenizer.setIncludedFields(props.getIncludedFields());
					}
					String line = reader.readLine();
					fields = tokenizer.tokenize(line).getValues();
					log.info("Found header {}", Arrays.asList(fields));
				} catch (Exception e) {
					log.error("Could not read header for file {}", resource, e);
				}
			}
		}
		if (fields == null) {
			throw new RechargeException("No fields specified for file " + resource);
		}
		builder.names(fields);
		return fileBuilder.build();
	}

	private FlatFileItemReader<Map<String, Object>> fixedLengthReader(Resource resource, FileProperties props)
			throws IOException {
		log.info("Reading fixed-length file {}", props);
		FlatFileItemReaderBuilder<Map<String, Object>> fileBuilder = flatFileBuilder(resource, props);
		fileBuilder.name("fixed-length-file-reader");
		FixedLengthBuilder<Map<String, Object>> builder = fileBuilder.fixedLength();
		if (props.getRanges() != null) {
			builder.columns(ranges(props.getRanges()));
		}
		if (props.getFields() != null) {
			builder.names(props.getFields());
		}
		return fileBuilder.build();
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
	private JsonItemReader<? extends Map> jsonReader(Resource resource, FileProperties props) {
		log.info("Reading JSON file {}", props);
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.resource(resource);
		builder.jsonObjectReader(jsonObjectReader());
		builder.name("json-file-reader");
		builder.strict(props.isStrict());
		return builder.build();
	}

	@SuppressWarnings("rawtypes")
	private JsonObjectReader<Map> jsonObjectReader() {
		JacksonJsonObjectReader<Map> reader = new JacksonJsonObjectReader<>(Map.class);
		reader.setMapper(new ObjectMapper());
		return reader;
	}

}
