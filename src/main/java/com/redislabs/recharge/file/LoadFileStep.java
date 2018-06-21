package com.redislabs.recharge.file;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.util.ResourceUtils;

import com.redislabs.recharge.batch.MapFieldSetMapper;
import com.redislabs.recharge.batch.RediSearchWriter;
import com.redislabs.recharge.batch.RedisWriter;
import com.redislabs.recharge.config.Delimited;
import com.redislabs.recharge.config.FileType;
import com.redislabs.recharge.config.FixedLength;
import com.redislabs.recharge.config.Recharge;

@Configuration
public class LoadFileStep {

	private Logger log = LoggerFactory.getLogger(LoadFileStep.class);

	private Pattern fileExtensionPattern = Pattern.compile(".*\\.(?<filetype>\\w+)(?<gz>\\.gz)?");

	@Autowired
	private Recharge config;

	@Autowired
	private RediSearchWriter rediSearchWriter;

	@Autowired
	private RedisWriter redisWriter;

	@Bean
	public FlatFileItemReader<Map<String, String>> fileReader() throws IOException {
		Resource resource = getResource(config.getFile().getPath());
		Matcher matcher = fileExtensionPattern.matcher(resource.getFilename());
		FileType type = config.getFile().getType();
		Boolean gzip = config.getFile().getGzip();
		if (matcher.find()) {
			if (type == null) {
				type = getFiletype(matcher.group("filetype"));
			}
			if (gzip == null) {
				String gz = matcher.group("gz");
				if (gz != null && gz.length() > 0) {
					gzip = true;
				}
			}
		}
		if (type == null) {
			log.info("Could not determine type of file {}; try specifying the type with --file.type=blah", resource);
			return null;
		}
		if (Boolean.TRUE.equals(gzip)) {
			resource = new GZIPResource(resource);
		}
		if (config.getKey().getPrefix() == null) {
			config.getKey().setPrefix(getBaseFilename(resource));
		}
		if (config.getKey().getFields() == null) {
			config.getKey().setFields(new String[] { config.getFile().getFlat().getFieldNames()[0] });
		}
		switch (type) {
		case FixedLength:
			return getFixedLengthItemReader(resource);
		default:
			return getDelimitedItemReader(resource);
		}
	}

	private Resource getResource(String path) throws MalformedURLException {
		if (ResourceUtils.isUrl(path)) {
			return new UrlResource(path);
		}
		return new FileSystemResource(path);
	}

	private String getBaseFilename(Resource resource) {
		int pos = resource.getFilename().indexOf(".");
		if (pos == -1) {
			return resource.getFilename();
		}
		return resource.getFilename().substring(0, pos);
	}

	private FileType getFiletype(String extension) {
		switch (extension) {
		case "fw":
			return FileType.FixedLength;
		case "csv":
			return FileType.Delimited;
		case "json":
			return FileType.JSON;
		case "xml":
			return FileType.XML;
		default:
			return FileType.Delimited;
		}
	}

	private FlatFileItemReaderBuilder<Map<String, String>> getFileReaderBuilder(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, String>> builder = new FlatFileItemReaderBuilder<Map<String, String>>();
		builder.name("file-reader");
		builder.resource(resource);
		if (config.getFile().getFlat().getLinesToSkip() != null) {
			builder.linesToSkip(config.getFile().getFlat().getLinesToSkip());
		}
		if (config.getFile().getEncoding() != null) {
			builder.encoding(config.getFile().getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		return builder;
	}

	private FlatFileItemReader<Map<String, String>> getDelimitedItemReader(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, String>> readerBuilder = getFileReaderBuilder(resource);
		Delimited delimited = config.getFile().getFlat().getDelimited();
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
		if (config.getFile().getFlat().getFieldNames() != null) {
			delimitedBuilder.names(config.getFile().getFlat().getFieldNames());
		}
		return readerBuilder.build();
	}

	private FlatFileItemReader<Map<String, String>> getFixedLengthItemReader(Resource resource) {
		FlatFileItemReaderBuilder<Map<String, String>> readerBuilder = getFileReaderBuilder(resource);
		FixedLength fixedLength = config.getFile().getFlat().getFixedLength();
		FixedLengthBuilder<Map<String, String>> fixedLengthBuilder = readerBuilder.fixedLength();
		if (fixedLength.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(fixedLength.getRanges()));
		}
		if (config.getFile().getFlat().getFieldNames() != null) {
			fixedLengthBuilder.names(config.getFile().getFlat().getFieldNames());
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

	public ItemWriter<Map<String, String>> writer() {
		if (config.getRedisearch().getIndex() != null) {
			rediSearchWriter.open();
			CompositeItemWriter<Map<String, String>> writer = new CompositeItemWriter<Map<String, String>>();
			writer.setDelegates(Arrays.asList(redisWriter, rediSearchWriter));
			return writer;
		}
		return redisWriter;
	}

}