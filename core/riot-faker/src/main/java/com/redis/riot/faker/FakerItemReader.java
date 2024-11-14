package com.redis.riot.faker;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import net.datafaker.Faker;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final Locale DEFAULT_LOCALE = Locale.getDefault();

	private Map<String, String> expressions = new LinkedHashMap<>();
	private Locale locale = DEFAULT_LOCALE;

	private Faker faker;
	private Map<String, String> fields;

	public FakerItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public void setExpressions(Map<String, String> fields) {
		this.expressions = fields;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		Assert.notEmpty(expressions, "No field specified");
		if (fields == null) {
			fields = expressions.entrySet().stream().map(this::normalizeField)
					.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
		}
		faker = new Faker(locale);
	}

	private Entry<String, String> normalizeField(Entry<String, String> field) {
		if (field.getValue().startsWith("#{")) {
			return field;
		}
		return new AbstractMap.SimpleEntry<>(field.getKey(), "#{" + field.getValue() + "}");
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		for (Entry<String, String> field : fields.entrySet()) {
			String value;
			synchronized (faker) {
				value = faker.expression(field.getValue());
			}
			map.put(field.getKey(), value);
		}
		return map;
	}

	@Override
	protected synchronized void doClose() {
		faker = null;
	}

}
