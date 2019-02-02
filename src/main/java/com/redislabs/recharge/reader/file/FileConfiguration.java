package com.redislabs.recharge.reader.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeConfiguration.DelimitedFileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileReaderConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileType;
import com.redislabs.recharge.RechargeConfiguration.FixedLengthFileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FlatFileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.JsonFileConfiguration;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
@SuppressWarnings("rawtypes")
public class FileConfiguration {

	private static final String FILE_BASENAME = "basename";
	private static final String FILE_EXTENSION = "extension";
	private static final String FILE_GZ = "gz";
	private Pattern filePathPattern = Pattern
			.compile("(?<" + FILE_BASENAME + ">.+)\\.(?<" + FILE_EXTENSION + ">\\w+)(?<" + FILE_GZ + ">\\.gz)?");

	@Autowired
	private RechargeConfiguration rechargeConfig;

	private BufferedReaderFactory bufferedReaderFactory = new DefaultBufferedReaderFactory();

	private FlatFileItemReaderBuilder<Map> getFlatFileReaderBuilder(Resource resource, FlatFileConfiguration fileConfig)
			throws IOException {
		FlatFileItemReaderBuilder<Map> builder;
		builder = new FlatFileItemReaderBuilder<Map>();
		builder.resource(resource);
		if (fileConfig.getEncoding() != null) {
			builder.encoding(fileConfig.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.linesToSkip(fileConfig.getLinesToSkip());
		builder.recordSeparatorPolicy(new DefaultRecordSeparatorPolicy());
		if (fileConfig.isHeader() && fileConfig.getLinesToSkip() == 0) {
			builder.linesToSkip(1);
		}
		return builder;
	}

	private Resource getResource(FileReaderConfiguration reader) throws IOException {
		Resource resource = getResource(reader.getPath());
		if (isGzip(reader)) {
			return getGZipResource(resource);
		}
		return resource;
	}

	private Resource getResource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private boolean isGzip(FileReaderConfiguration reader) {
		if (reader.getGzip() == null) {
			String gz = getFilenameGroup(reader.getPath(), FILE_GZ);
			return gz != null && gz.length() > 0;
		}
		return reader.getGzip();
	}

	private String getFilenameGroup(String path, String groupName) {
		Matcher matcher = filePathPattern.matcher(getFilename(path));
		if (matcher.find()) {
			return matcher.group(groupName);
		}
		return null;
	}

	private String getFilename(String path) {
		return new File(path).getName();
	}

	private Resource getGZipResource(Resource resource) throws IOException {
		return new InputStreamResource(new GZIPInputStream(resource.getInputStream()));
	}

	private FlatFileItemReader<Map> getDelimitedReader(DelimitedFileConfiguration delimited, Resource resource)
			throws IOException {
		FlatFileItemReaderBuilder<Map> builder = getFlatFileReaderBuilder(resource, delimited);
		DelimitedBuilder<Map> delimitedBuilder = builder.delimited();
		if (delimited.getDelimiter() != null) {
			delimitedBuilder.delimiter(delimited.getDelimiter());
		}
		if (delimited.getIncludedFields() != null) {
			delimitedBuilder.includedFields(delimited.getIncludedFields());
		}
		if (delimited.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(delimited.getQuoteCharacter());
		}
		String[] fieldNames = delimited.getFields();
		if (delimited.isHeader()) {
			try {
				BufferedReader reader = bufferedReaderFactory.create(resource, delimited.getEncoding());
				DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
				if (delimited.getDelimiter() != null) {
					tokenizer.setDelimiter(delimited.getDelimiter());
				}
				if (delimited.getQuoteCharacter() != null) {
					tokenizer.setQuoteCharacter(delimited.getQuoteCharacter());
				}
				String line = reader.readLine();
				FieldSet fields = tokenizer.tokenize(line);
				fieldNames = fields.getValues();
				log.info("Found header {}", Arrays.toString(fieldNames));
			} catch (Exception e) {
				log.error("Could not read header for file {}", resource, e);
			}
		}
		delimitedBuilder.names(fieldNames);
		return builder.build();
	}

	private FlatFileItemReader<Map> getFixedLengthReader(FixedLengthFileConfiguration config, Resource resource)
			throws IOException {
		FlatFileItemReaderBuilder<Map> builder = getFlatFileReaderBuilder(resource, config);
		FixedLengthBuilder<Map> fixedLengthBuilder = builder.fixedLength();
		if (config.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(config.getRanges()));
		}
		if (config.getStrict() != null) {
			fixedLengthBuilder.strict(config.getStrict());
		}
		fixedLengthBuilder.names(config.getFields());
		return builder.build();
	}

	public String getBaseName(FileReaderConfiguration config) {
		String filename = new File(config.getPath()).getName();
		int extensionIndex = filename.lastIndexOf(".");
		if (extensionIndex == -1) {
			return filename;
		}
		return filename.substring(0, extensionIndex);
	}

	private Range[] getRanges(String[] strings) {
		Range[] ranges = new Range[strings.length];
		for (int index = 0; index < strings.length; index++) {
			ranges[index] = getRange(strings[index]);
		}
		return ranges;
	}

	private Range getRange(String string) {
		String[] split = string.split("-");
		return new Range(Integer.parseInt(split[0]), Integer.parseInt(split[1]));
	}

	public AbstractItemCountingItemStreamItemReader<Map> reader(FileReaderConfiguration reader) throws IOException {
		Resource resource = getResource(reader);
		if (reader.getType() == null) {
			reader.setType(getFileType(resource));
		}
		switch (reader.getType()) {
		case FixedLength:
			return getFixedLengthReader(reader.getFixedLength(), resource);
		case Json:
			return getJsonReader(reader.getJson(), resource);
		default:
			return getDelimitedReader(reader.getDelimited(), resource);
		}
	}

	private FileType getFileType(Resource resource) {
		String extension = getFilenameGroup(resource.getFilename(), FILE_EXTENSION);
		if (extension == null) {
			return null;
		}
		log.debug("Found file extension '{}' for path {}", extension, resource);
		return rechargeConfig.getFileTypes().get(extension);
	}

	private AbstractItemCountingItemStreamItemReader<Map> getJsonReader(JsonFileConfiguration config,
			Resource resource) {
		JacksonJsonObjectReader<Map> reader = new JacksonJsonObjectReader<>(Map.class);
		reader.setMapper(new ObjectMapper());
		JsonItemReaderBuilder<Map> builder = new JsonItemReaderBuilder<>();
		builder.resource(resource);
		builder.jsonObjectReader(reader);
		builder.name("jsonreader");
		return builder.build();
	}
}
