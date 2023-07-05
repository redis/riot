package com.redis.riot.core;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.redis.spring.batch.common.DataStructure;
import com.redis.spring.batch.reader.KeyComparison;
import com.redis.spring.batch.reader.KeyComparison.Status;

class DiffTests {

	private final KeyComparisonLogger comparisonLogger = new KeyComparisonLogger();

	@Test
	void testHashDiff() {
		String key = "mykey";
		DataStructure<String> source = new DataStructure<>();
		source.setKey(key);
		source.setTtl(123L);
		source.setType(DataStructure.HASH);
		Map<String, String> hash1 = new HashMap<>();
		hash1.put("field1", "value1");
		hash1.put("field2", "value2");
		source.setValue(hash1);
		DataStructure<String> target = new DataStructure<>();
		target.setKey(key);
		target.setTtl(12345L);
		target.setType(DataStructure.HASH);
		Map<String, String> hash2 = new HashMap<>();
		hash2.put("field1", "value1");
		hash2.put("field2", "value2");
		hash2.put("field3", "value3");
		target.setValue(hash2);
		KeyComparison comparison = new KeyComparison(source, target, Status.VALUE);
		comparisonLogger.log(comparison);
	}

	@Test
	void testStringDiff() {
		String key = "mykey";
		DataStructure<String> source = new DataStructure<>();
		source.setKey(key);
		source.setTtl(123L);
		source.setType(DataStructure.STRING);
		source.setValue("value");
		DataStructure<String> target = new DataStructure<>();
		target.setKey(key);
		target.setTtl(12345L);
		target.setType(DataStructure.STRING);
		target.setValue("value2");
		KeyComparison comparison = new KeyComparison(source, target, Status.VALUE);
		comparisonLogger.log(comparison);
	}

}
