package org.springframework.core.convert.support;

import org.springframework.core.convert.converter.Converter;

public class ConverterFactory {

	private static final StringToNumberConverterFactory STRING_TO_NUMBER_CONVERTER_FACTORY = new StringToNumberConverterFactory();
	private static final NumberToNumberConverterFactory NUMBER_TO_NUMBER_CONVERTER_FACTORY = new NumberToNumberConverterFactory();

	public static Converter<Object, String> getObjectToStringConverter() {
		return new ObjectToStringConverter();
	}

	public static <T extends Number> Converter<String, T> getStringToNumberConverter(Class<T> targetType) {
		return STRING_TO_NUMBER_CONVERTER_FACTORY.getConverter(targetType);
	}

	public static <T extends Number> Converter<Number, T> getNumberToNumberConverter(Class<T> targetType) {
		return NUMBER_TO_NUMBER_CONVERTER_FACTORY.getConverter(targetType);
	}

}
