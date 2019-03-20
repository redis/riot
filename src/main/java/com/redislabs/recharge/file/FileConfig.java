package com.redislabs.recharge.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.ArrayUtils;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.recharge.RechargeConfiguration;

import lombok.extern.slf4j.Slf4j;

@Configuration
@Slf4j
public class FileConfig {

	private static final String FILE_BASENAME = "basename";
	private static final String FILE_EXTENSION = "extension";
	private static final String FILE_GZ = "gz";
	private static final Pattern filePathPattern = Pattern
			.compile("(?<" + FILE_BASENAME + ">.+)\\.(?<" + FILE_EXTENSION + ">\\w+)(?<" + FILE_GZ + ">\\.gz)?");

	@Autowired
	private RechargeConfiguration config;

	private FlatFileItemReaderBuilder<Map<String, Object>> getFlatFileReaderBuilder(Resource resource,
			FlatFileConfiguration fileConfig) throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder;
		builder = new FlatFileItemReaderBuilder<Map<String, Object>>();
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

	private Resource resource(FileSourceConfiguration file) throws IOException {
		Resource resource = resource(file.getPath());
		if (isGzip(file)) {
			return getGZipResource(resource);
		}
		return resource;
	}

	private Resource resource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private boolean isGzip(FileSourceConfiguration file) {
		if (file.getGzip() == null) {
			String gz = getFilenameGroup(file.getPath(), FILE_GZ);
			return gz != null && gz.length() > 0;
		}
		return file.getGzip();
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

	private FlatFileItemReader<Map<String, Object>> getDelimitedReader(DelimitedFileConfiguration delimited,
			Resource resource) throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> fileBuilder = getFlatFileReaderBuilder(resource, delimited);
		DelimitedBuilder<Map<String, Object>> builder = fileBuilder.delimited();
		if (delimited.getDelimiter() != null) {
			builder.delimiter(delimited.getDelimiter());
		}
		if (delimited.getIncludedFields() != null) {
			builder.includedFields(ArrayUtils.toObject(delimited.getIncludedFields()));
		}
		if (delimited.getQuoteCharacter() != null) {
			builder.quoteCharacter(delimited.getQuoteCharacter());
		}
		if (delimited.isHeader()) {
			fileBuilder.linesToSkip(1);
			if (delimited.getFields() == null) {
				try {
					BufferedReader reader = new DefaultBufferedReaderFactory().create(resource,
							delimited.getEncoding());
					DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
					if (delimited.getDelimiter() != null) {
						tokenizer.setDelimiter(delimited.getDelimiter());
					}
					if (delimited.getQuoteCharacter() != null) {
						tokenizer.setQuoteCharacter(delimited.getQuoteCharacter());
					}
					if (delimited.getIncludedFields() != null) {
						tokenizer.setIncludedFields(delimited.getIncludedFields());
					}
					String line = reader.readLine();
					delimited.setFields(tokenizer.tokenize(line).getValues());
					log.info("Found header {}", Arrays.asList(delimited.getFields()));
				} catch (Exception e) {
					log.error("Could not read header for file {}", resource, e);
				}
			}
		}
		if (delimited.getFields() != null) {
			builder.names(delimited.getFields());
			config.getSink().getRedis().setCollectionFields(delimited.getFields());
		}
		return fileBuilder.build();
	}

	private FlatFileItemReader<Map<String, Object>> getFixedLengthReader(FixedLengthFileConfiguration fixedLength,
			Resource resource) throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> fileBuilder = getFlatFileReaderBuilder(resource, fixedLength);
		FixedLengthBuilder<Map<String, Object>> builder = fileBuilder.fixedLength();
		if (fixedLength.getRanges() != null) {
			builder.columns(getRanges(fixedLength.getRanges()));
		}
		if (fixedLength.getStrict() != null) {
			builder.strict(fixedLength.getStrict());
		}
		if (fixedLength.getFields() != null) {
			builder.names(fixedLength.getFields());
			config.getSink().getRedis().setCollectionFields(fixedLength.getFields());
		}
		return fileBuilder.build();
	}

	public String baseName(FileSourceConfiguration file) {
		String filename = new File(file.getPath()).getName();
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

	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader() {
		AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader = reader(config.getSource().getFile());
		String baseName = baseName(config.getSource().getFile());
		config.getSink().getRedis().setKeyspace(baseName);
		if (config.getSink().getRedis().getFt() != null) {
			if (config.getSink().getRedis().getFt().getAdd().getIndex() == null) {
				config.getSink().getRedis().getFt().getAdd().setIndex(baseName);
			}
		}
		return reader;
	}

	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> reader(FileSourceConfiguration file) {
		try {
			Resource resource = resource(file);
			if (file.getType() == null) {
				file.setType(getFileType(resource));
			}
			switch (file.getType()) {
			case FixedLength:
				return getFixedLengthReader(file.getFixedLength(), resource);
			case Json:
				return getJsonReader(file.getJson(), resource);
			default:
				return getDelimitedReader(file.getDelimited(), resource);
			}
		} catch (IOException e) {
			log.error("Could not create file reader for {}", file.getPath());
			return null;
		}
	}

	private FileType getFileType(Resource resource) {
		String extension = getFilenameGroup(resource.getFilename(), FILE_EXTENSION);
		if (extension == null) {
			return null;
		}
		log.debug("Found file extension '{}' for path {}", extension, resource);
		return config.getFileTypes().get(extension);
	}

	private JsonItemReader<Map<String, Object>> getJsonReader(JsonFileConfiguration json, Resource resource) {
		JacksonJsonObjectReader<Map<String, Object>> reader = new JacksonJsonObjectReader<>(Map.class);
		reader.setMapper(new ObjectMapper());
		JsonItemReaderBuilder<Map<String, Object>> builder = new JsonItemReaderBuilder<>();
		builder.resource(resource);
		builder.strict(json.isStrict());
		builder.jsonObjectReader(reader);
		builder.name("jsonreader");
		return builder.build();
	}

}
