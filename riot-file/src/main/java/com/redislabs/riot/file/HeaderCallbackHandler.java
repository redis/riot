package com.redislabs.riot.file;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.file.LineCallbackHandler;
import org.springframework.batch.item.file.transform.AbstractLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HeaderCallbackHandler implements LineCallbackHandler {

    private final AbstractLineTokenizer tokenizer;

    public HeaderCallbackHandler(AbstractLineTokenizer tokenizer) {
        this.tokenizer = tokenizer;
    }

    @Override
    public void handleLine(String line) {
        log.info("Found header {}", line);
        FieldSet fieldSet = tokenizer.tokenize(line);
        List<String> fields = new ArrayList<>();
        for (int index = 0; index < fieldSet.getFieldCount(); index++) {
            fields.add(fieldSet.readString(index));
        }
        tokenizer.setNames(fields.toArray(new String[0]));
    }
}