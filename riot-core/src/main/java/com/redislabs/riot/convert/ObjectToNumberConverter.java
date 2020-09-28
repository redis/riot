package com.redislabs.riot.convert;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.support.ConverterFactory;

public class ObjectToNumberConverter<T extends Number> implements Converter<Object, T> {

	private TypeDescriptor targetTypeDescriptor;
	private Converter<Number, T> numberConverter;
	private Converter<String, T> stringConverter;

	public ObjectToNumberConverter(Class<T> targetType) {
		this.targetTypeDescriptor = TypeDescriptor.valueOf(targetType);
		this.numberConverter = ConverterFactory.getNumberToNumberConverter(targetType);
		this.stringConverter = ConverterFactory.getStringToNumberConverter(targetType);
	}

	@Override
	public T convert(Object source) {
		if (source instanceof Number) {
			return numberConverter.convert((Number) source);
		}
		if (source instanceof String) {
			return stringConverter.convert((String) source);
		}
		throw new ConversionFailedException(TypeDescriptor.forObject(source), targetTypeDescriptor, source, null);
	}

}
