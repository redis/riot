package com.redislabs.riot.convert;

import lombok.Builder;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;
import org.springframework.util.Assert;

public interface KeyMaker<T> extends Converter<T, String> {

    String DEFAULT_SEPARATOR = ":";

    String EMPTY_STRING = "";

    static <T> KeyConverterBuilder<T> builder() {
        return new KeyConverterBuilder<>();
    }

    @Accessors(fluent = true)
    @SuppressWarnings("unchecked")
    public static class KeyConverterBuilder<T> {
        @Setter
        private String separator = DEFAULT_SEPARATOR;
        @Setter
        private String prefix = EMPTY_STRING;
		private Converter<T, String>[] keyExtractors = new Converter[0];

        public KeyConverterBuilder<T> extractors(Converter<T, String>... keyExtractors) {
            this.keyExtractors = keyExtractors;
            return this;
        }

        private String getPrefix() {
            if (prefix == null || prefix.isEmpty()) {
                return EMPTY_STRING;
            }
            return prefix + separator;
        }

        public KeyMaker<T> build() {
            if (keyExtractors == null || keyExtractors.length == 0) {
                Assert.isTrue(prefix != null && !prefix.isEmpty(), "No keyspace nor key fields specified");
                return NoKeyMaker.<T>builder().prefix(prefix).build();
            }
            if (keyExtractors.length == 1) {
                return SingleKeyMaker.<T>builder().prefix(getPrefix()).keyExtractor(keyExtractors[0]).build();
            }
            return MultiKeyMaker.<T>builder().prefix(getPrefix()).separator(separator).keyExtractors(keyExtractors).build();
        }

    }

    @Builder
    public static class NoKeyMaker<T> implements KeyMaker<T> {

        @NonNull
        private final String prefix;

        @Override
        public String convert(T source) {
            return prefix;
        }
    }


    @Builder
    public static class SingleKeyMaker<T> implements KeyMaker<T> {

        @NonNull
        private final String prefix;
        @NonNull
        private final Converter<T, String> keyExtractor;

        @Override
        public String convert(T source) {
            return prefix + keyExtractor.convert(source);
        }
    }

    @Builder
    public static class MultiKeyMaker<T> implements KeyMaker<T> {

        @NonNull
        private final String prefix;
        @NonNull
        @Builder.Default
        private final String separator = DEFAULT_SEPARATOR;
        @NonNull
        private final Converter<T, String>[] keyExtractors;

        @Override
        public String convert(T source) {
            StringBuilder builder = new StringBuilder();
            builder.append(prefix);
            for (int index = 0; index < keyExtractors.length - 1; index++) {
                builder.append(keyExtractors[index].convert(source));
                builder.append(separator);
            }
            builder.append(keyExtractors[keyExtractors.length - 1].convert(source));
            return builder.toString();
        }

    }

}
