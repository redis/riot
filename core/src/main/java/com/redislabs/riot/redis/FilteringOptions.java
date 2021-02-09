package com.redislabs.riot.redis;

import com.redislabs.riot.convert.CompositeConverter;
import com.redislabs.riot.convert.MapFilteringConverter;
import com.redislabs.riot.convert.MapFlattener;
import com.redislabs.riot.convert.ObjectToStringConverter;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import org.springframework.core.convert.converter.Converter;
import picocli.CommandLine;

import java.util.Map;

@Builder
@AllArgsConstructor
@NoArgsConstructor
public class FilteringOptions {

    @Builder.Default
    @CommandLine.Option(arity = "1..*", names = "--include", description = "Name(s) of fields to include", paramLabel = "<field>")
    private String[] includes = new String[0];
    @Builder.Default
    @CommandLine.Option(arity = "1..*", names = "--exclude", description = "Name(s) of fields to exclude", paramLabel = "<field>")
    private String[] excludes = new String[0];

    public Converter<Map<String, Object>, Map<String, String>> converter() {
        MapFlattener<String> mapFlattener = new MapFlattener<>(new ObjectToStringConverter());
        if (includes.length == 0 && excludes.length == 0) {
            return mapFlattener;
        }
        return new CompositeConverter(mapFlattener, MapFilteringConverter.<String, Object>builder().includes(includes).excludes(excludes).build());
    }


}
