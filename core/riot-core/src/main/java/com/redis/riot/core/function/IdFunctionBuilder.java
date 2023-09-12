package com.redis.riot.core.function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.springframework.util.CollectionUtils;

public class IdFunctionBuilder {

    public static final String DEFAULT_SEPARATOR = ":";

    private String separator = DEFAULT_SEPARATOR;

    private String prefix;

    private final FieldExtractorFactory extractorFactory = FieldExtractorFactory.builder().nullCheck(true).build();

    private final List<String> fields = new ArrayList<>();

    public IdFunctionBuilder remove(boolean remove) {
        this.extractorFactory.setRemove(remove);
        return this;
    }

    public IdFunctionBuilder fields(String... fields) {
        return fields(Arrays.asList(fields));
    }

    public IdFunctionBuilder fields(List<String> fields) {
        if (!CollectionUtils.isEmpty(fields)) {
            this.fields.addAll(fields);
        }
        return this;
    }

    public IdFunctionBuilder prefix(String prefix) {
        this.prefix = prefix;
        return this;
    }

    public IdFunctionBuilder separator(String separator) {
        this.separator = separator;
        return this;
    }

    public Function<Map<String, Object>, String> build() {
        if (fields.isEmpty()) {
            if (prefix != null) {
                return m -> prefix;
            }
            throw new IllegalArgumentException("No prefix and no fields specified");
        }
        if (fields.size() == 1) {
            Function<Map<String, Object>, String> extractor = extractorFactory.string(fields.get(0));
            if (prefix != null) {
                return s -> prefix + separator + extractor.apply(s);
            }
            return extractor::apply;
        }
        List<Function<Map<String, Object>, String>> toStringFunctions = new ArrayList<>();
        if (prefix != null) {
            toStringFunctions.add(s -> prefix);
        }
        for (String field : fields) {
            toStringFunctions.add(extractorFactory.string(field));
        }
        return new ConcatenatingFunction(separator, toStringFunctions);
    }

    public static class ConcatenatingFunction implements Function<Map<String, Object>, String> {

        private final String separator;

        private final List<Function<Map<String, Object>, String>> toStringFunctions;

        public ConcatenatingFunction(String separator, List<Function<Map<String, Object>, String>> toStringFunctions) {
            this.separator = separator;
            this.toStringFunctions = toStringFunctions;
        }

        @Override
        public String apply(Map<String, Object> source) {
            if (source == null) {
                return null;
            }
            StringBuilder builder = new StringBuilder();
            builder.append(toStringFunctions.get(0).apply(source));
            for (int index = 1; index < toStringFunctions.size(); index++) {
                builder.append(separator);
                builder.append(toStringFunctions.get(index).apply(source));
            }
            return builder.toString();
        }

    }

}
