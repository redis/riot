package com.redislabs.riot.processor;

import com.redislabs.lettusearch.suggest.Suggestion;
import org.springframework.batch.item.ItemProcessor;

import java.util.HashMap;
import java.util.Map;

public class SuggestionMapItemProcessor implements ItemProcessor<Suggestion<String>, Map<String, String>> {

    public final static String FIELD_STRING = "string";
    public final static String FIELD_SCORE = "score";
    public final static String FIELD_PAYLOAD = "payload";

    @Override
    public Map<String, String> process(Suggestion<String> item) {
        Map<String, String> map = new HashMap<>();
        map.put(FIELD_STRING, item.getString());
        map.put(FIELD_SCORE, String.valueOf(item.getScore()));
        if (item.getPayload() != null) {
            map.put(FIELD_PAYLOAD, String.valueOf(item.getPayload()));
        }
        return map;
    }
}
