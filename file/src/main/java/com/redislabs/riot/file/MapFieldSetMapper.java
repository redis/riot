package com.redislabs.riot.file;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

import java.util.HashMap;
import java.util.Map;

public class MapFieldSetMapper implements FieldSetMapper<Map<String, String>> {

    @Override
    public Map<String, String> mapFieldSet(FieldSet fieldSet) {
        Map<String, String> fields = new HashMap<>();
        String[] names = fieldSet.getNames();
        for (int index = 0; index < names.length; index++) {
            String name = names[index];
            String value = fieldSet.readString(index);
            if (value == null || value.length() == 0) {
                continue;
            }
            fields.put(name, value);
        }
        return fields;
    }
}