package com.redis.riot.file;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder.FormattedBuilder;
import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.redis.riot.file.xml.XmlResourceItemWriterBuilder;

public abstract class FileWriterFactories {

	public static FlatFileItemWriter<Map<String, Object>> delimited(WritableResource resource,
			FileWriterOptions options) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource, options);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = writer.delimited();
		delimitedBuilder.delimiter(options.getFileOptions().getDelimiter().orElseGet(() -> Files.delimiter(resource)));
		delimitedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		delimitedBuilder.quoteCharacter(String.valueOf(options.getFileOptions().getQuoteCharacter()));
		return flatFileWriter(writer, delimitedBuilder.build(), options);
	}

	public static FlatFileItemWriter<Map<String, Object>> formatted(WritableResource resource,
			FileWriterOptions options) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource, options);
		FormattedBuilder<Map<String, Object>> formattedBuilder = writer.formatted();
		formattedBuilder.format(options.getFormatterString());
		formattedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		return flatFileWriter(writer, formattedBuilder.build(), options);
	}

	public static FlatFileItemWriter<?> jsonLines(WritableResource resource, FileWriterOptions options) {
		FlatFileItemWriterBuilder<?> builder = flatFileWriter(resource, options);
		builder.lineAggregator(new JsonLineAggregator<>(new ObjectMapper()));
		return builder.build();
	}

	public static ItemWriter<?> json(WritableResource resource, FileWriterOptions options) {
		JsonFileItemWriterBuilder<?> writer = new JsonFileItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.resource(resource);
		writer.append(options.isAppend());
		writer.encoding(options.getFileOptions().getEncoding());
		writer.forceSync(options.isForceSync());
		writer.lineSeparator(options.getLineSeparator());
		writer.saveState(false);
		writer.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
		writer.shouldDeleteIfExists(options.isShouldDeleteIfExists());
		writer.transactional(options.isTransactional());
		writer.jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>(objectMapper(new ObjectMapper())));
		return writer.build();
	}

	private static <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
		return objectMapper;
	}

	public static ItemWriter<?> xml(WritableResource resource, FileWriterOptions options) {
		XmlResourceItemWriterBuilder<?> writer = new XmlResourceItemWriterBuilder<>();
		writer.name(resource.getFilename());
		writer.append(options.isAppend());
		writer.encoding(options.getFileOptions().getEncoding());
		writer.lineSeparator(options.getLineSeparator());
		writer.rootName(options.getRootName());
		writer.resource(resource);
		writer.saveState(false);
		XmlMapper mapper = objectMapper(new XmlMapper());
		mapper.setConfig(mapper.getSerializationConfig().withRootName(options.getElementName()));
		writer.xmlObjectMarshaller(new JacksonJsonObjectMarshaller<>(mapper));
		return writer.build();
	}

	private static <T> FlatFileItemWriterBuilder<T> flatFileWriter(WritableResource resource,
			FileWriterOptions options) {
		FlatFileItemWriterBuilder<T> builder = new FlatFileItemWriterBuilder<>();
		builder.name(resource.getFilename());
		builder.resource(resource);
		builder.append(options.isAppend());
		builder.encoding(options.getFileOptions().getEncoding());
		builder.forceSync(options.isForceSync());
		builder.lineSeparator(options.getLineSeparator());
		builder.saveState(false);
		builder.shouldDeleteIfEmpty(options.isShouldDeleteIfEmpty());
		builder.shouldDeleteIfExists(options.isShouldDeleteIfExists());
		builder.transactional(options.isTransactional());
		return builder;
	}

	private static FlatFileItemWriter<Map<String, Object>> flatFileWriter(
			FlatFileItemWriterBuilder<Map<String, Object>> writer, LineAggregator<Map<String, Object>> aggregator,
			FileWriterOptions options) {
		writer.lineAggregator(aggregator);
		if (options.getFileOptions().isHeader()) {
			Map<String, Object> headerRecord = options.getHeaderSupplier().get();
			if (!CollectionUtils.isEmpty(headerRecord)) {
				List<String> fields = new ArrayList<>(headerRecord.keySet());
				Collections.sort(fields);
				Map<String, Object> fieldMap = new LinkedHashMap<>();
				fields.forEach(f -> fieldMap.put(f, f));
				String headerLine = aggregator.aggregate(fieldMap);
				writer.headerCallback(w -> w.write(headerLine));
			}
		}
		return writer.build();
	}

}
