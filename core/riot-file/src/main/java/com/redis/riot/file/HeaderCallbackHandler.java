package com.redis.riot.file;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;

public class HeaderCallbackHandler implements LineCallbackHandler {

	private final Log log = LogFactory.getLog(getClass());

	private final AbstractLineTokenizer tokenizer;
	private final int headerIndex;

	private int lineIndex;

	public HeaderCallbackHandler(AbstractLineTokenizer tokenizer, int headerIndex) {
		this.tokenizer = tokenizer;
		this.headerIndex = headerIndex;
	}

	@Override
	public void handleLine(String line) {
		if (lineIndex == headerIndex) {
			log.info("Found header: " + line);
			FieldSet fieldSet = tokenizer.tokenize(line);
			List<String> fields = new ArrayList<>();
			for (int index = 0; index < fieldSet.getFieldCount(); index++) {
				fields.add(fieldSet.readString(index));
			}
			log.info("Using field names: " + fields);
			tokenizer.setNames(fields.toArray(new String[0]));
		}
		lineIndex++;
	}

}
