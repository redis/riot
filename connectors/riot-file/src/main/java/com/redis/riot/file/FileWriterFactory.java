package com.redis.riot.file;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlResourceItemWriter;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;
import com.redis.riot.resource.FlatFileItemWriter;
import com.redis.riot.resource.FlatFileItemWriterBuilder;
import com.redis.riot.resource.FlatFileItemWriterBuilder.DelimitedBuilder;
import com.redis.riot.resource.FlatFileItemWriterBuilder.FormattedBuilder;
import com.redis.riot.resource.JsonFileItemWriter;
import com.redis.riot.resource.JsonFileItemWriterBuilder;

public class FileWriterFactory {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private FileWriterArgs options = new FileWriterArgs();
	private Supplier<Map<String, Object>> headerSupplier = () -> null;

	@SuppressWarnings("unchecked")
	public <T> ItemWriter<T> create(String file) {
		WritableResource resource = options.resource(file);
		FileType type = options.fileType(resource);
		switch (type) {
		case CSV:
			return (ItemWriter<T>) delimitedWriter(resource);
		case FIXED:
			return (ItemWriter<T>) fixedLengthWriter(resource);
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

	private <T> FlatFileItemWriter<T> jsonlWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<T> builder = flatFileWriter(resource);
		builder.lineAggregator(new JsonLineAggregator<>(objectMapper(new ObjectMapper())));
		return builder.build();
	}

	private <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
		return objectMapper;
	}

	private <T> JsonFileItemWriter<T> jsonWriter(WritableResource resource) {
		JsonFileItemWriterBuilder<T> writer = new JsonFileItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(options.isAppend());
		writer.encoding(options.getEncoding());
		writer.lineSeparator(options.getLineSeparator());
		writer.resource(resource);
		writer.saveState(false);
		ObjectMapper mapper = objectMapper(new ObjectMapper());
		writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private <T> XmlResourceItemWriter<T> xmlWriter(WritableResource resource) {
		XmlResourceItemWriterBuilder<T> writer = new XmlResourceItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(options.isAppend());
		writer.encoding(options.getEncoding());
		writer.lineSeparator(options.getLineSeparator());
		writer.rootName(options.getRootName());
		writer.resource(resource);
		writer.saveState(false);
		XmlMapper mapper = objectMapper(new XmlMapper());
		mapper.setConfig(mapper.getSerializationConfig().withRootName(options.getElementName()));
		writer.xmlObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private ItemWriter<Map<String, Object>> delimitedWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = writer.delimited();
		delimitedBuilder.delimiter(options.getDelimiter());
		delimitedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		delimitedBuilder.quoteCharacter(String.valueOf(options.getQuoteCharacter()));
		return writer(writer, delimitedBuilder.build());
	}

	private FlatFileItemWriter<Map<String, Object>> writer(FlatFileItemWriterBuilder<Map<String, Object>> writer,
			LineAggregator<Map<String, Object>> lineAggregator) {
		writer.lineAggregator(lineAggregator);
		if (options.isHeader()) {
			Map<String, Object> headerRecord = headerSupplier.get();
			if (CollectionUtils.isEmpty(headerRecord)) {
				log.warn("Could not determine header");
			} else {
				Map<String, Object> headerFieldMap = new HashMap<>();
				headerRecord.forEach((k, v) -> headerFieldMap.put(k, k));
				String headerLine = lineAggregator.aggregate(headerFieldMap);
				log.info("Found header: {}", headerLine);
				writer.headerCallback(w -> w.write(headerLine));
			}
		}
		return writer.build();
	}

	private ItemWriter<Map<String, Object>> fixedLengthWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource);
		FormattedBuilder<Map<String, Object>> formattedBuilder = writer.formatted();
		formattedBuilder.format(options.getFormatterString());
		formattedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		return writer(writer, formattedBuilder.build());
	}

	private <T> FlatFileItemWriterBuilder<T> flatFileWriter(WritableResource resource) {
		FlatFileItemWriterBuilder<T> builder = new FlatFileItemWriterBuilder<>();
		builder.name(resource.getFilename());
		builder.resource(resource);
		builder.append(options.isAppend());
		builder.encoding(options.getEncoding());
		builder.forceSync(options.isForceSync());
		builder.lineSeparator(options.getLineSeparator());
		builder.saveState(false);
		builder.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
		builder.shouldDeleteIfExists(options.isShouldDeleteIfExists());
		builder.transactional(options.isTransactional());
		return builder;
	}

	public FileWriterArgs getOptions() {
		return options;
	}

	public void setOptions(FileWriterArgs options) {
		this.options = options;
	}

	public Supplier<Map<String, Object>> getHeaderSupplier() {
		return headerSupplier;
	}

	public void setHeaderSupplier(Supplier<Map<String, Object>> supplier) {
		this.headerSupplier = supplier;
	}

}
