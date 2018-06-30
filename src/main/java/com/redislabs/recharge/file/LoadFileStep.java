package com.redislabs.recharge.file;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import com.redislabs.recharge.MapItemProcessor;
import com.redislabs.recharge.RechargeException;
import com.redislabs.recharge.config.KeyConfiguration;

@Configuration
public class LoadFileStep {

	@Autowired
	private FileConfiguration config;

	@Autowired
	private KeyConfiguration keyConfig;

	public FlatFileItemReader<Map<String, String>> reader() throws Exception {
		FileType type = config.getType();
		if (type == null) {
			throw new RechargeException("Could not determine type of file " + config.getPath());
		}
		Resource resource = getResource();
		switch (type) {
		case FixedLength:
			return getFixedLengthItemReader(resource);
		default:
			return getDelimitedItemReader(resource);
		}
	}

	private Resource getResource() throws IOException {
		Resource resource = getResource(config.getPath());
		if (Boolean.TRUE.equals(config.getGzip())) {
			return new GZIPResource(resource);
		}
		return resource;
	}

	private Resource getResource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private String getBaseFilename() {
		String filename = new File(config.getPath()).getName();
		int pos = filename.indexOf(".");
		if (pos == -1) {
			return filename;
		}
		return filename.substring(0, pos);
	}

	private FlatFileItemReaderBuilder<Map<String, String>> getFileReaderBuilder(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<>();
		builder.name("file-reader");
		builder.resource(resource);
		if (config.getFlat().getLinesToSkip() != null) {
			builder.linesToSkip(config.getFlat().getLinesToSkip());
		}
		if (config.getEncoding() != null) {
			builder.encoding(config.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		return builder;
	}

	public MapItemProcessor processor() {
		String[] fieldNames = config.getFlat().getFieldNames();
		String keyPrefix = keyConfig.getPrefix();
		if (keyPrefix == null) {
			keyPrefix = getBaseFilename();
		}
		String keySeparator = keyConfig.getSeparator();
		String[] keyFields = keyConfig.getFields();
		if (keyConfig.getFields() == null || keyConfig.getFields().length == 0) {
			keyFields = new String[] { fieldNames[0] };
		}
		return new MapItemProcessor(keyPrefix, keyFields, keySeparator);
	}

	private FlatFileItemReader<Map<String, String>> getDelimitedItemReader(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, String>> readerBuilder = getFileReaderBuilder(resource);
		DelimitedConfiguration delimited = config.getFlat().getDelimited();
		DelimitedBuilder<Map<String, String>> delimitedBuilder = readerBuilder.delimited();
		if (delimited.getDelimiter() != null) {
			delimitedBuilder.delimiter(delimited.getDelimiter());
		}
		if (delimited.getIncludedFields() != null) {
			delimitedBuilder.includedFields(delimited.getIncludedFields());
		}
		if (delimited.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(delimited.getQuoteCharacter());
		}
		if (config.getFlat().getFieldNames() != null) {
			delimitedBuilder.names(config.getFlat().getFieldNames());
		}
		return readerBuilder.build();
	}

	private FlatFileItemReader<Map<String, String>> getFixedLengthItemReader(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, String>> readerBuilder = getFileReaderBuilder(resource);
		FixedLengthConfiguration fixedLength = config.getFlat().getFixedLength();
		FixedLengthBuilder<Map<String, String>> fixedLengthBuilder = readerBuilder.fixedLength();
		if (fixedLength.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(fixedLength.getRanges()));
		}
		if (config.getFlat().getFieldNames() != null) {
			fixedLengthBuilder.names(config.getFlat().getFieldNames());
		}
		if (fixedLength.getStrict() != null) {
			fixedLengthBuilder.strict(fixedLength.getStrict());
		}
		return readerBuilder.build();
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

}