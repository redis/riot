package com.redislabs.riot.processor;

import com.redislabs.lettusearch.search.Document;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;

public class DocumentMapItemProcessor implements ItemProcessor<Document<String, String>, Map<String, String>> {

    public final static String FIELD_ID = "id";
    public final static String FIELD_SCORE = "score";
    public final static String FIELD_PAYLOAD = "payload";

    @Override
    public Map<String, String> process(Document<String, String> item) {
        Map<String, String> map = new HashMap<>();
        map.put(FIELD_ID, item.getId());
        map.put(FIELD_SCORE, String.valueOf(item.getScore()));
        map.putAll(item);
        if (item.getPayload() != null) {
            map.put(FIELD_PAYLOAD, String.valueOf(item.getPayload()));
        }
        return map;
    }
}
