package com.redislabs.riot.batch;

import java.lang.reflect.Method;
import java.text.DateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionInvocationTargetException;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import com.redislabs.riot.generator.GeneratorReader;

public class SpelProcessor implements ItemProcessor<Map<String, Object>, Map<String, Object>>, Map<String, Object> {

	private final Logger log = LoggerFactory.getLogger(SpelProcessor.class);

	private SpelExpressionParser parser = new SpelExpressionParser();
	private StandardEvaluationContext context;
	private Map<String, Expression> expressions = new LinkedHashMap<>();
	private long index = 0;

	public SpelProcessor(Object redis, DateFormat dateFormat, Map<String, String> variables,
			Map<String, String> fields) {
		context = new StandardEvaluationContext();
		context.setVariable("redis", redis);
		context.setVariable("date", dateFormat);
		context.setVariable("context", this);
		variables.forEach((k, v) -> context.setVariable(k, parser.parseExpression(v).getValue(context)));
		Method geoMethod;
		try {
			geoMethod = getClass().getDeclaredMethod("geo", new Class[] { String.class, String.class });
			context.registerFunction("geo", geoMethod);
		} catch (NoSuchMethodException | SecurityException e) {
			log.error("Could not register geo function", e);
		}
		context.setPropertyAccessors(Arrays.asList(new MapAccessor()));
		fields.forEach((k, v) -> expressions.put(k, parser.parseExpression(v)));
	}

	public Map<String, Object> process(Map<String, Object> item) throws Exception {
		synchronized (context) {
			for (Entry<String, Expression> entry : expressions.entrySet()) {
				try {
					Object value = entry.getValue().getValue(context, item);
					if (value != null) {
						item.put(entry.getKey(), value);
					}
				} catch (ExpressionInvocationTargetException e) {
					log.error("Error while evaluating field {} with item {}", entry.getKey(), item, e);
				}
			}
			index++;
		}
		return item;
	}

	protected static String geo(String longitude, String latitude) {
		if (longitude == null || latitude == null) {
			return null;
		}
		return longitude + "," + latitude;
	}

	@Override
	public int size() {
		return 1;
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean containsKey(Object key) {
		return true;
	}

	@Override
	public boolean containsValue(Object value) {
		return true;
	}

	@Override
	public Object get(Object key) {
		if (GeneratorReader.FIELD_INDEX.equals(key)) {
			return index;
		}
		return null;
	}

	@Override
	public Object put(String key, Object value) {
		return null;
	}

	@Override
	public Object remove(Object key) {
		return null;
	}

	@Override
	public void putAll(Map<? extends String, ? extends Object> m) {
	}

	@Override
	public void clear() {
	}

	@Override
	public Set<String> keySet() {
		return null;
	}

	@Override
	public Collection<Object> values() {
		return null;
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return null;
	}

}
