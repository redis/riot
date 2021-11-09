package com.redis.riot.file;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;

public class HeaderCallbackHandler implements LineCallbackHandler {

	private static final Logger log = LoggerFactory.getLogger(HeaderCallbackHandler.class);

	private final AbstractLineTokenizer tokenizer;

	public HeaderCallbackHandler(AbstractLineTokenizer tokenizer) {
		this.tokenizer = tokenizer;
	}

	@Override
	public void handleLine(String line) {
		log.debug("Found header {}", line);
		FieldSet fieldSet = tokenizer.tokenize(line);
		List<String> fields = new ArrayList<>();
		for (int index = 0; index < fieldSet.getFieldCount(); index++) {
			fields.add(fieldSet.readString(index));
		}
		tokenizer.setNames(fields.toArray(new String[0]));
	}
}