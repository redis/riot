package com.redislabs.riot.processor;

import com.redislabs.lettusearch.search.Document;
import com.redislabs.riot.WritableDocument;
import org.springframework.batch.item.ItemProcessor;

public class DocumentWritableProcessor implements ItemProcessor<Document<String, String>, WritableDocument> {

    @Override
    public WritableDocument process(Document<String, String> item) throws Exception {
        WritableDocument document = new WritableDocument();
        document.setId(item.getId());
        document.setScore(item.getScore());
        document.setPayload(item.getPayload());
        document.setFields(item);
        return document;
    }
}
