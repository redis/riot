package com.redislabs.recharge.redis;

import java.util.Map;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

import com.redislabs.recharge.RechargeConfiguration.KeyConfiguration;

public class KeyBuilder {

	private ConversionService conversionService = new DefaultConversionService();
	private String separator;
	private String prefix;
	private IValueAccessor accessor;

	public KeyBuilder(KeyConfiguration config) {
		this.prefix = config.getPrefix() == null ? config.getFields()[0] : config.getPrefix();
		this.separator = config.getSeparator();
		this.accessor = (config.getFields() == null || config.getFields().length == 0) ? new FirstValueAccessor()
				: new ValueAccessor(config.getFields());
	}

	public String getKey(Map<String, Object> map) {
		return prefix + separator + getId(map);
	}

	public String getId(Map<String, Object> map) {
		return getId(getKeyFieldValues(map));
	}

	private Object[] getKeyFieldValues(Map<String, Object> map) {
		return accessor.getValueArray(map);
	}

	private String getId(Object[] values) {
		String id = toString(values[0]);
		for (int index = 1; index < values.length; index++) {
			id += separator + toString(values[index]);
		}
		return id;
	}

	private String toString(Object value) {
		return conversionService.convert(value, String.class);
	}
}
