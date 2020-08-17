package com.redislabs.riot.convert.field;

import com.redislabs.riot.convert.StringToDoubleConverter;
import com.redislabs.riot.convert.StringToIntegerConverter;
import com.redislabs.riot.convert.StringToLongConverter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.springframework.core.convert.converter.Converter;

import java.util.Map;

public abstract class FieldExtractor<K, V> implements Converter<Map<K, V>, V> {

    private final K field;

    protected FieldExtractor(K field) {
        this.field = field;
    }

    @Override
    public V convert(Map<K, V> source) {
        return getValue(source, field);
    }

    abstract protected V getValue(Map<K, V> source, K field);

    public static FieldExtractorBuilder<String> builder() {
        return new FieldExtractorBuilder<>(String.class);
    }

    public static <T> FieldExtractorBuilder<T> builder(Class<T> targetType) {
        return new FieldExtractorBuilder<>(targetType);
    }

    @Accessors(fluent = true)
    @Setter
    public static class FieldExtractorBuilder<T> {

        private final Class<T> targetType;

        private String field;
        private boolean remove;
        private T defaultValue;

        public FieldExtractorBuilder(Class<T> targetType) {
            this.targetType = targetType;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
		public Converter<Map<String, String>, T> build() {
            if (field == null) {
                if (defaultValue == null) {
                    return null;
                }
                return new ConstantFieldExtractor<>(defaultValue);
            }
            if (String.class.isAssignableFrom(targetType)) {
                if (defaultValue == null) {
                    return (Converter) fieldExtractor();
                }
                return (Converter) new DefaultingFieldExtractor<>(field, (String) defaultValue);
            }
            return new DefaultingCompositeConverter<>(fieldExtractor(), defaultValue, (Converter) stringToTarget(targetType));
        }

        private Converter<String, ?> stringToTarget(Class<?> targetType) {
            if (targetType.isAssignableFrom(Long.class)) {
                return new StringToLongConverter();
            }
            if (targetType.isAssignableFrom(Integer.class)) {
                return new StringToIntegerConverter();
            }
            if (targetType.isAssignableFrom(Double.class)) {
                return new StringToDoubleConverter();
            }
            throw new IllegalArgumentException("No converter found for type " + targetType.getName());
        }

        private Converter<Map<String, String>, String> fieldExtractor() {
            if (remove) {
                return new RemovingFieldExtractor<>(field);
            }
            return new SimpleFieldExtractor<>(field);
        }

    }
}
