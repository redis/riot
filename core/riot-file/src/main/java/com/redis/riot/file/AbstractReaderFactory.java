package com.redis.riot.file;

import java.util.Map;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public abstract class AbstractReaderFactory implements ReaderFactory {

	protected int[] includedFields(ReadOptions options) {
		return options.getIncludedFields().stream().mapToInt(Integer::intValue).toArray();
	}

	protected FlatFileItemReader<Map<String, Object>> flatFileReader(Resource resource, ReadOptions options,
			AbstractLineTokenizer tokenizer) {
		if (ObjectUtils.isEmpty(options.getFields())) {
			Assert.isTrue(options.isHeader(),
					String.format("Could not create reader for file '%s': no header or field names specified",
							resource.getFilename()));
		} else {
			tokenizer.setNames(options.getFields().toArray(new String[0]));
		}
		FlatFileItemReaderBuilder<Map<String, Object>> builder = flatFileReader(options);
		builder.resource(resource);
		builder.fieldSetMapper(new MapFieldSetMapper());
		builder.lineTokenizer(tokenizer);
		builder.skippedLinesCallback(new HeaderCallbackHandler(tokenizer, headerIndex(options)));
		return builder.build();
	}

	protected <T> FlatFileItemReaderBuilder<T> flatFileReader(ReadOptions options) {
		FlatFileItemReaderBuilder<T> builder = new FlatFileItemReaderBuilder<>();
		if (options.getMaxItemCount() > 0) {
			builder.maxItemCount(options.getMaxItemCount());
		}
		builder.encoding(options.getEncoding());
		builder.recordSeparatorPolicy(recordSeparatorPolicy(options));
		builder.linesToSkip(linesToSkip(options));
		builder.saveState(false);
		return builder;
	}

	private RecordSeparatorPolicy recordSeparatorPolicy(ReadOptions options) {
		String quoteCharacter = String.valueOf(options.getQuoteCharacter());
		return new DefaultRecordSeparatorPolicy(quoteCharacter, options.getContinuationString());
	}

	private int headerIndex(ReadOptions options) {
		if (options.getHeaderLine() != null) {
			return options.getHeaderLine();
		}
		return linesToSkip(options) - 1;
	}

	private int linesToSkip(ReadOptions options) {
		if (options.getLinesToSkip() != null) {
			return options.getLinesToSkip();
		}
		if (options.isHeader()) {
			return 1;
		}
		return 0;
	}

	protected <T extends ObjectMapper> T objectMapper(T objectMapper, ReadOptions options) {
		objectMapper.configure(DeserializationFeature.USE_LONG_FOR_INTS, true);
		SimpleModule module = new SimpleModule();
		options.getDeserializers().forEach(module::addDeserializer);
		objectMapper.registerModule(module);
		return objectMapper;
	}

}
