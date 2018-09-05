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

import org.springframework.batch.item.file.BufferedReaderFactory;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

import com.redislabs.recharge.MapFieldSetMapper;
import com.redislabs.recharge.RechargeConfiguration;
import com.redislabs.recharge.RechargeException;
import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileConfiguration;
import com.redislabs.recharge.RechargeConfiguration.FileType;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class FileLoadConfiguration {

	private static final String FILE_BASENAME = "basename";
	private static final String FILE_EXTENSION = "extension";
	private static final String FILE_GZ = "gz";
	private Pattern filePathPattern = Pattern
			.compile("(?<" + FILE_BASENAME + ">.+)\\.(?<" + FILE_EXTENSION + ">\\w+)(?<" + FILE_GZ + ">\\.gz)?");

	@Autowired
	private RechargeConfiguration rechargeConfig;

	private BufferedReaderFactory bufferedReaderFactory = new DefaultBufferedReaderFactory();

	private FlatFileItemReaderBuilder<Map<String, Object>> getFlatFileReaderBuilder(EntityConfiguration entity)
			throws IOException {
		FlatFileItemReaderBuilder<Map<String, Object>> builder = new FlatFileItemReaderBuilder<>();
		Resource resource = getResource(entity);
		builder.resource(resource);
		if (entity.getName() == null) {
			entity.setName(getFileBaseName(resource));
		}
		FileConfiguration fileConfig = entity.getFileConfig();
		if (fileConfig.getEncoding() != null) {
			builder.encoding(fileConfig.getEncoding());
		}
		builder.strict(true);
		builder.saveState(false);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.linesToSkip(fileConfig.getLinesToSkip());
		if (fileConfig.isHeader() && fileConfig.getLinesToSkip() == 0) {
			builder.linesToSkip(1);
		}
		return builder;
	}

	private Resource getResource(EntityConfiguration entity) throws IOException {
		Resource resource = getResource(entity.getFile());
		if (isGzip(entity)) {
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

	private boolean isGzip(EntityConfiguration entity) {
		if (entity.getFileConfig().getGzip() == null) {
			String gz = getFilenameGroup(entity.getFile(), FILE_GZ);
			return gz != null && gz.length() > 0;
		}
		return entity.getFileConfig().getGzip();
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

	private FlatFileItemReader<Map<String, Object>> getDelimitedReader(EntityConfiguration entity) throws IOException {
		FileConfiguration config = entity.getFileConfig();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = getFlatFileReaderBuilder(entity);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = builder.delimited();
		if (config.getDelimiter() != null) {
			delimitedBuilder.delimiter(config.getDelimiter());
		}
		if (config.getIncludedFields() != null) {
			delimitedBuilder.includedFields(config.getIncludedFields());
		}
		if (config.getQuoteCharacter() != null) {
			delimitedBuilder.quoteCharacter(config.getQuoteCharacter());
		}
		String[] fieldNames = entity.getFields();
		if (config.isHeader()) {
			Resource resource = getResource(entity);
			try {
				BufferedReader reader = bufferedReaderFactory.create(resource, config.getEncoding());
				DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
				if (config.getDelimiter() != null) {
					tokenizer.setDelimiter(config.getDelimiter());
				}
				if (config.getQuoteCharacter() != null) {
					tokenizer.setQuoteCharacter(config.getQuoteCharacter());
				}
				String line = reader.readLine();
				FieldSet fields = tokenizer.tokenize(line);
				fieldNames = fields.getValues();
				log.info("Found header {}", Arrays.toString(fieldNames));
			} catch (Exception e) {
				log.error("Could not read header for file {}", entity.getFile(), e);
			}
		}
		delimitedBuilder.names(fieldNames);
		return builder.build();
	}

	private FlatFileItemReader<Map<String, Object>> getFixedLengthReader(EntityConfiguration entity)
			throws IOException {
		FileConfiguration config = entity.getFileConfig();
		FlatFileItemReaderBuilder<Map<String, Object>> builder = getFlatFileReaderBuilder(entity);
		FixedLengthBuilder<Map<String, Object>> fixedLengthBuilder = builder.fixedLength();
		if (config.getRanges() != null) {
			fixedLengthBuilder.columns(getRanges(config.getRanges()));
		}
		if (config.getStrict() != null) {
			fixedLengthBuilder.strict(config.getStrict());
		}
		fixedLengthBuilder.names(entity.getFields());
		return builder.build();
	}

	private String getFileBaseName(Resource resource) {
		String filename = resource.getFilename();
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

	private FileType guessFileType(String path) {
		String extension = getFilenameGroup(path, FILE_EXTENSION);
		if (extension == null) {
			log.warn("Could not determine file type from path {}", path);
		}
		log.debug("Found file extension '{}' for path {}", extension, path);
		return rechargeConfig.getFileTypes().getOrDefault(extension, FileType.Delimited);
	}

	public AbstractItemCountingItemStreamItemReader<Map<String, Object>> getReader(EntityConfiguration entity)
			throws IOException, RechargeException {
		FileConfiguration fileConfig = entity.getFileConfig();
		if (fileConfig.getType() == null) {
			fileConfig.setType(guessFileType(entity.getFile()));
		}
		switch (fileConfig.getType()) {
		case Delimited:
			return getDelimitedReader(entity);
		case FixedLength:
			return getFixedLengthReader(entity);
		case Json:
			return new JsonItemReader();
		default:
			throw new RechargeException("No reader found for file " + entity.getFile());
		}
	}
}
