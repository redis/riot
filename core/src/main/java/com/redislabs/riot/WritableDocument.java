package com.redislabs.riot;

import lombok.Data;

import java.util.Map;

@Data
public class WritableDocument {

    public String id;
    public Double score;
    public Map<String, String> fields;
    private String payload;
}
