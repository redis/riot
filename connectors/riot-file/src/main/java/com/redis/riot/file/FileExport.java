package com.redis.riot.file;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.FormattedBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.core.AbstractExport;
import com.redis.riot.core.KeyValueMapProcessorOptions;
import com.redis.riot.core.RiotUtils;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.spring.batch.RedisItemReader;
import com.redis.spring.batch.reader.MemKeyValue;

import io.lettuce.core.codec.StringCodec;

public class FileExport extends AbstractExport {

	public static final String DEFAULT_LINE_SEPARATOR = System.getProperty("line.separator");
	public static final String DEFAULT_ELEMENT_NAME = "record";
	public static final String DEFAULT_ROOT_NAME = "root";
	public static final String DEFAULT_QUOTE_CHARACTER = "";
	public static final String DEFAULT_DELIMITER = ",";
	public static final ContentType DEFAULT_CONTENT_TYPE = ContentType.MAP;

	private String file;
	private FileOptions fileOptions = new FileOptions();
	private FileType fileType;
	private String formatterString;
	private ContentType contentType = DEFAULT_CONTENT_TYPE;
	private String rootName = DEFAULT_ROOT_NAME;
	private String elementName = DEFAULT_ELEMENT_NAME;
	private boolean append;
	private boolean forceSync;
	private String lineSeparator = DEFAULT_LINE_SEPARATOR;
	private boolean shouldDeleteIfEmpty;
	private boolean shouldDeleteIfExists;
	private String quoteCharacter = DEFAULT_QUOTE_CHARACTER;
	private String delimiter = DEFAULT_DELIMITER;
	private List<String> fields;
	private KeyValueMapProcessorOptions mapProcessorOptions = new KeyValueMapProcessorOptions();

	@SuppressWarnings("unchecked")
	@Override
	protected Job job() throws IOException {
		RedisItemReader<String, String, MemKeyValue<String, Object>> reader = RedisItemReader.struct();
		configure(reader);
		return jobBuilder().start(step(getName(), reader, writer()).processor(processor()).build()).build();
	}

	@SuppressWarnings("rawtypes")
	private ItemProcessor processor() {
		if (contentType == ContentType.REDIS) {
			return processor(StringCodec.UTF8);
		}
		return RiotUtils.processor(processor(StringCodec.UTF8), mapProcessorOptions.processor());
	}

	@SuppressWarnings("rawtypes")
	private ItemWriter writer() throws IOException {
		WritableResource resource = FileUtils.outputResource(file, fileOptions);
		FileType type = fileType(resource);
		if (type == null) {
			throw new IllegalArgumentException("No file type specified");
		}
		switch (type) {
		case DELIMITED:
			return delimitedWriter(resource);
		case FIXED_LENGTH:
			return fixedLengthWriter(resource);
		case JSON:
			return jsonWriter(resource);
		case JSONL:
			return jsonlWriter(resource);
		case XML:
			return xmlWriter(resource);
		default:
			throw new UnsupportedOperationException("Unsupported file type: " + type);
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private FlatFileItemWriter jsonlWriter(WritableResource resource) {
		FlatFileItemWriterBuilder builder = flatFileWriter(resource);
		builder.lineAggregator(new JsonLineAggregator(objectMapper(new ObjectMapper())));
		return builder.build();
	}

	private <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
		return objectMapper;
	}

	private static class JsonLineAggregator<T> implements LineAggregator<T> {

		private final Logger log = LoggerFactory.getLogger(getClass());

		private final ObjectMapper mapper;

		public JsonLineAggregator(ObjectMapper mapper) {
			this.mapper = mapper;
		}

		@Override
		public String aggregate(T item) {
			try {
				return mapper.writeValueAsString(item);
			} catch (JsonProcessingException e) {
				log.error("Cpuld not serialize item", e);
				return null;
			}
		}
	}

	private <T> JsonFileItemWriter<T> jsonWriter(WritableResource resource) {
		JsonFileItemWriterBuilder<T> writer = new JsonFileItemWriterBuilder<>();
		writer.name(file);
		writer.append(append);
		writer.encoding(fileOptions.getEncoding());
		writer.lineSeparator(lineSeparator);
		writer.resource(resource);
		writer.saveState(false);
		ObjectMapper mapper = objectMapper(new ObjectMapper());
		writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private <T> XmlResourceItemWriter<T> xmlWriter(WritableResource resource) {
		XmlResourceItemWriterBuilder<T> writer = new XmlResourceItemWriterBuilder<>();
		writer.name(file);
		writer.append(append);
		writer.encoding(fileOptions.getEncoding());
		writer.lineSeparator(lineSeparator);
		writer.rootName(rootName);
		writer.resource(resource);
		writer.saveState(false);
		XmlMapper mapper = objectMapper(new XmlMapper());
		mapper.setConfig(mapper.getSerializationConfig().withRootName(elementName));
		writer.xmlObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private ItemWriter<Map<String, Object>> delimitedWriter(WritableResource resource) {
		if (contentType != ContentType.MAP) {
			throw new UnsupportedOperationException(
					String.format("Content type %s is not supported for delimited files", contentType));
		}
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource);
		DelimitedBuilder<Map<String, Object>> lineAggregator = writer.delimited();
		lineAggregator.delimiter(delimiter);
		lineAggregator.fieldExtractor(new PassThroughFieldExtractor<>());
		if (!CollectionUtils.isEmpty(fields)) {
			lineAggregator.names(fields.toArray(new String[0]));
		}
		lineAggregator.quoteCharacter(quoteCharacter);
		writer.lineAggregator(lineAggregator.build());
		return writer.build();
	}

	private ItemWriter<Map<String, Object>> fixedLengthWriter(WritableResource resource) {
		if (contentType != ContentType.MAP) {
			throw new UnsupportedOperationException(
					String.format("Content type %s is not supported for fixed-length files", contentType));
		}
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource);
		FormattedBuilder<Map<String, Object>> lineAggregator = writer.formatted();
		lineAggregator.format(formatterString);
		lineAggregator.fieldExtractor(new PassThroughFieldExtractor<>());
		if (!CollectionUtils.isEmpty(fields)) {
			lineAggregator.names(fields.toArray(new String[0]));
		}
		writer.lineAggregator(lineAggregator.build());
		return writer.build();

	}

	private FlatFileItemWriterBuilder<Map<String, Object>> flatFileWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> builder = new FlatFileItemWriterBuilder<>();
		builder.name(file);
		builder.resource(resource);
		builder.append(append);
		builder.encoding(fileOptions.getEncoding());
		builder.forceSync(forceSync);
		builder.lineSeparator(lineSeparator);
		builder.saveState(false);
		builder.shouldDeleteIfEmpty(shouldDeleteIfEmpty);
		builder.shouldDeleteIfExists(shouldDeleteIfExists);
		return builder;
	}

	private FileType fileType(WritableResource resource) {
		if (fileType == null) {
			return FileUtils.fileType(resource);
		}
		return fileType;
	}

	public String getFile() {
		return file;
	}

	public void setFile(String file) {
		this.file = file;
	}

	public FileOptions getFileOptions() {
		return fileOptions;
	}

	public void setFileOptions(FileOptions options) {
		this.fileOptions = options;
	}

	public FileType getFileType() {
		return fileType;
	}

	public void setFileType(FileType type) {
		this.fileType = type;
	}

	public String getRootName() {
		return rootName;
	}

	public void setRootName(String name) {
		this.rootName = name;
	}

	public String getElementName() {
		return elementName;
	}

	public void setElementName(String name) {
		this.elementName = name;
	}

	public boolean isAppend() {
		return append;
	}

	public void setAppend(boolean append) {
		this.append = append;
	}

	public boolean isForceSync() {
		return forceSync;
	}

	public void setForceSync(boolean forceSync) {
		this.forceSync = forceSync;
	}

	public String getLineSeparator() {
		return lineSeparator;
	}

	public void setLineSeparator(String separator) {
		this.lineSeparator = separator;
	}

	public boolean isShouldDeleteIfEmpty() {
		return shouldDeleteIfEmpty;
	}

	public void setShouldDeleteIfEmpty(boolean shouldDeleteIfEmpty) {
		this.shouldDeleteIfEmpty = shouldDeleteIfEmpty;
	}

	public boolean isShouldDeleteIfExists() {
		return shouldDeleteIfExists;
	}

	public void setShouldDeleteIfExists(boolean shouldDeleteIfExists) {
		this.shouldDeleteIfExists = shouldDeleteIfExists;
	}

	public String getQuoteCharacter() {
		return quoteCharacter;
	}

	public void setQuoteCharacter(String character) {
		this.quoteCharacter = character;
	}

	public String getDelimiter() {
		return delimiter;
	}

	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}

	public List<String> getFields() {
		return fields;
	}

	public void setFields(List<String> fields) {
		this.fields = fields;
	}

	public KeyValueMapProcessorOptions getMapProcessorOptions() {
		return mapProcessorOptions;
	}

	public void setMapProcessorOptions(KeyValueMapProcessorOptions options) {
		this.mapProcessorOptions = options;
	}

	public String getFormatterString() {
		return formatterString;
	}

	public void setFormatterString(String formatterString) {
		this.formatterString = formatterString;
	}

	public ContentType getContentType() {
		return contentType;
	}

	public void setContentType(ContentType contentType) {
		this.contentType = contentType;
	}

}
