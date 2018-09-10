package com.redislabs.recharge.file;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.Test;
import org.springframework.core.io.InputStreamResource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.recharge.file.JacksonUnmarshaller;
import com.redislabs.recharge.file.JsonItemReader;

public class JsonEntityReaderTests {

	public static class TestObject {
		private Integer id;
		private String string;
		private Double doubleValue;
		private Boolean booleanValue;
		private List<TestObject> nestedTestObjectList;

		public void setId(Integer id) {
			this.id = id;
		}

		public Integer getId() {
			return this.id;
		}

		public void getString(String string) {
			this.string = string;
		}

		public String getString() {
			return this.string;
		}

		public void setDoubleValue(Double doubleValue) {
			this.doubleValue = doubleValue;
		}

		public Double getDoubleValue() {
			return this.doubleValue;
		}

		public void setBooleanValue(Boolean booleanValue) {
			this.booleanValue = booleanValue;
		}

		public Boolean getBooleanValue() {
			return this.booleanValue;
		}

		public void setNestedTestObjectList(List<TestObject> nestedTestObjectList) {
			this.nestedTestObjectList = nestedTestObjectList;
		}

		public List<TestObject> getNestedTestObjectList() {
			return this.nestedTestObjectList;
		}
	}

	@Test
	public void callData() throws Exception {
		JacksonUnmarshaller unmarshaller = new JacksonUnmarshaller();
		unmarshaller.setObjectMapper(new ObjectMapper());
		JsonItemReader itemReader = new JsonItemReader();
		itemReader.setResource(new InputStreamResource(ClassLoader.class.getResourceAsStream("/json/CallData.json")));
		itemReader.setUnmarshaller(unmarshaller);
		itemReader.setKeyName("data");
		itemReader.afterPropertiesSet();
		itemReader.doOpen();
		Map<String, Object> entity = itemReader.read();
		print(null, entity);
	}

	private void print(String prefix, Map<String, Object> map) {
		for (Entry<String, Object> entry : map.entrySet()) {
			printValue(getProperty(prefix, entry.getKey()), entry.getValue());
		}
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void printValue(String property, Object value) {
		if (value instanceof Map) {
			print(property, (Map) value);
		} else {
			if (value instanceof Collection) {
				int index = 0;
				for (Object object : (Collection) value) {
					printValue(property + "[" + index + "]", object);
					index++;
				}
			} else {
				System.out.println(property + "=" + String.valueOf(value));
			}
		}
	}

	private String getProperty(String prefix, String key) {
		if (prefix == null) {
			return key;
		}
		return prefix + "." + key;
	}
}