package com.redis.riot.file;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.core.io.Resource;
import org.springframework.util.ObjectUtils;

public class DelimitedReaderFactory extends AbstractReaderFactory {

	private final String delimiter;

	public DelimitedReaderFactory(String delimiter) {
		this.delimiter = delimiter;
	}

	@Override
	public ItemReader<?> create(Resource resource, ReadOptions options) {
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setDelimiter(options.getDelimiter() == null ? delimiter : options.getDelimiter());
		tokenizer.setQuoteCharacter(options.getQuoteCharacter());
		if (!ObjectUtils.isEmpty(options.getIncludedFields())) {
			tokenizer.setIncludedFields(includedFields(options));
		}
		return flatFileReader(resource, options, tokenizer);
	}

}
