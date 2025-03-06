package com.redis.riot.file;

import java.util.Map;

import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.transform.PassThroughFieldExtractor;
import org.springframework.core.io.WritableResource;

import com.redis.spring.batch.resource.FlatFileItemWriterBuilder;
import com.redis.spring.batch.resource.FlatFileItemWriterBuilder.DelimitedBuilder;

public class DelimitedWriterFactory extends AbstractWriterFactory {

	private final String delimiter;

	public DelimitedWriterFactory(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public ItemWriter<?> create(WritableResource resource, WriteOptions options) {
		FlatFileItemWriterBuilder<Map<String, Object>> writer = flatFileWriter(resource, options);
		DelimitedBuilder<Map<String, Object>> delimitedBuilder = writer.delimited();
		delimitedBuilder.delimiter(options.getDelimiter() == null ? delimiter : options.getDelimiter());
		delimitedBuilder.fieldExtractor(new PassThroughFieldExtractor<>());
		delimitedBuilder.quoteCharacter(String.valueOf(options.getQuoteCharacter()));
		return flatFileWriter(options, writer, delimitedBuilder.build());
	}

}
