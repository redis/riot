package com.redis.riot;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hrakaroo.glob.GlobPattern;
import com.redis.riot.core.function.StringToMapFunction;
import com.redis.spring.batch.Range;
import com.redis.spring.batch.item.redis.common.DataType;
import com.redis.spring.batch.item.redis.common.KeyValue;

import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.codec.StringCodec;

class ProcessorTests {

	private Predicate<String> keyFilter(KeyFilterArgs args) {
		return args.predicate(StringCodec.UTF8).get();
	}

	@Test
	void keyFilter() {
		KeyFilterArgs options = new KeyFilterArgs();
		options.setIncludes(Arrays.asList("foo*", "bar*"));
		Predicate<String> predicate = keyFilter(options);
		Assertions.assertTrue(predicate.test("foobar"));
		Assertions.assertTrue(predicate.test("barfoo"));
		Assertions.assertFalse(predicate.test("key"));
	}

	@Test
	void slotExact() {
		KeyFilterArgs options = new KeyFilterArgs();
		options.setSlots(Arrays.asList(Range.of(7638)));
		Predicate<String> predicate = keyFilter(options);
		assertTrue(predicate.test("abc"));
		assertFalse(predicate.test("abcd"));
	}

	@Test
	void slotRange() {
		KeyFilterArgs options = new KeyFilterArgs();
		options.setSlots(slotRangeList(0, SlotHash.SLOT_COUNT));
		Predicate<String> unbounded = keyFilter(options);
		assertTrue(unbounded.test("foo"));
		assertTrue(unbounded.test("foo1"));
		options.setSlots(slotRangeList(999999, 99999));
		Predicate<String> is999999 = keyFilter(options);
		assertFalse(is999999.test("foo"));
	}

	private List<Range> slotRangeList(int start, int end) {
		return Arrays.asList(Range.of(start, end));
	}

	@Test
	void kitchenSink() {
		KeyFilterArgs options = new KeyFilterArgs();
		options.setExcludes(Arrays.asList("foo"));
		options.setIncludes(Arrays.asList("foo1"));
		options.setSlots(Arrays.asList(Range.of(0, SlotHash.SLOT_COUNT)));
		Predicate<String> predicate = keyFilter(options);
		assertFalse(predicate.test("foo"));
		assertFalse(predicate.test("bar"));
		assertTrue(predicate.test("foo1"));
	}

	@Test
	void keyValueToMap() {
		KeyValueMapProcessor processor = new KeyValueMapProcessor();
		KeyValue<String, Object> string = new KeyValue<>();
		string.setKey("beer:1");
		string.setType(DataType.STRING.getString());
		String value = "sdfsdf";
		string.setValue(value);
		Map<String, ?> stringMap = processor.process(string);
		Assertions.assertEquals(value, stringMap.get(StringToMapFunction.DEFAULT_KEY));
		KeyValue<String, Object> hash = new KeyValue<>();
		hash.setKey("beer:2");
		hash.setType(DataType.HASH.getString());
		Map<String, String> map = new HashMap<>();
		map.put("field1", "value1");
		hash.setValue(map);
		Map<String, ?> hashMap = processor.process(hash);
		Assertions.assertEquals("value1", hashMap.get("field1"));
	}

	private Predicate<String> globPredicate(String match) {
		return GlobPattern.compile(match)::matches;
	}

	@Test
	void include() {
		Predicate<String> foo = globPredicate("foo");
		assertTrue(foo.test("foo"));
		assertFalse(foo.test("bar"));
		Predicate<String> fooStar = globPredicate("foo*");
		assertTrue(fooStar.test("foobar"));
		assertFalse(fooStar.test("barfoo"));
	}

	@Test
	void exclude() {
		Predicate<String> foo = globPredicate("foo").negate();
		assertFalse(foo.test("foo"));
		assertTrue(foo.test("foa"));
		Predicate<String> fooStar = globPredicate("foo*").negate();
		assertFalse(fooStar.test("foobar"));
		assertTrue(fooStar.test("barfoo"));
	}

	@Test
	void includeAndExclude() {
		Predicate<String> foo1 = globPredicate("foo1").and(globPredicate("foo").negate());
		assertFalse(foo1.test("foo"));
		assertFalse(foo1.test("bar"));
		assertTrue(foo1.test("foo1"));
		Predicate<String> foo1Star = globPredicate("foo").and(globPredicate("foo1*").negate());
		assertTrue(foo1Star.test("foo"));
		assertFalse(foo1Star.test("bar"));
		assertFalse(foo1Star.test("foo1"));
	}

}
