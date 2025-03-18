package com.redis.riot.file;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.file.transform.LineAggregator;
import org.springframework.core.io.WritableResource;
import org.springframework.util.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redis.spring.batch.resource.FlatFileItemWriter;
import com.redis.spring.batch.resource.FlatFileItemWriterBuilder;

public abstract class AbstractWriterFactory implements WriterFactory {

	protected <T extends ObjectMapper> T objectMapper(T objectMapper) {
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.setSerializationInclusion(Include.NON_DEFAULT);
		return objectMapper;
	}

	protected <T> FlatFileItemWriterBuilder<T> flatFileWriter(WritableResource resource, WriteOptions options) {
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

	protected FlatFileItemWriter<Map<String, Object>> flatFileWriter(WriteOptions options,
			FlatFileItemWriterBuilder<Map<String, Object>> writer, LineAggregator<Map<String, Object>> aggregator) {
		writer.lineAggregator(aggregator);
		if (options.isHeader()) {
			Map<String, Object> headerRecord = options.getHeaderSupplier().get();
			if (!CollectionUtils.isEmpty(headerRecord)) {
				List<String> fields = new ArrayList<>(headerRecord.keySet());
				Map<String, Object> fieldMap = new LinkedHashMap<>();
				fields.forEach(f -> fieldMap.put(f, f));
				String headerLine = aggregator.aggregate(fieldMap);
				writer.headerCallback(w -> w.write(headerLine));
			}
		}
		return writer.build();
	}

}
