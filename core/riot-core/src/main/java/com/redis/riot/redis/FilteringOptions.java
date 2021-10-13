package com.redis.riot.redis;

import com.redis.riot.convert.CompositeConverter;
import com.redis.riot.convert.MapFilteringConverter;
import com.redis.riot.convert.MapFlattener;
import com.redis.riot.convert.ObjectToStringConverter;
import lombok.Data;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;

import java.util.Map;

@Data
public class FilteringOptions {

    @CommandLine.Option(arity = "1..*", names = "--include", description = "Fields to include", paramLabel = "<field>")
    private String[] includes;
    @CommandLine.Option(arity = "1..*", names = "--exclude", description = "Fields to exclude", paramLabel = "<field>")
    private String[] excludes;

    @SuppressWarnings("unchecked")
	public Converter<Map<String, Object>, Map<String, String>> converter() {
        MapFlattener<String> mapFlattener = new MapFlattener<>(new ObjectToStringConverter());
        if (ObjectUtils.isEmpty(includes) && ObjectUtils.isEmpty(excludes)) {
            return mapFlattener;
        }
        MapFilteringConverter.MapFilteringConverterBuilder<String, Object> filtering = MapFilteringConverter.builder();
        if (!ObjectUtils.isEmpty(includes)) {
            filtering.includes(includes);
        }
        if (!ObjectUtils.isEmpty(excludes)) {
            filtering.excludes(excludes);
        }
        return new CompositeConverter(mapFlattener, filtering.build());
    }


}
