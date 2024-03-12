package com.redis.riot.faker;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.DataBindingMethodResolver;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.redis.riot.core.RiotUtils;

import net.datafaker.Faker;

/**
 * {@link ItemReader} that generates HashMaps using Faker.
 *
 * @author Julien Ruaux
 */
public class FakerItemReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public static final Locale DEFAULT_LOCALE = Locale.getDefault();

	private Map<String, Expression> fields = new LinkedHashMap<>();
	private Locale locale = DEFAULT_LOCALE;
	private StandardEvaluationContext evaluationContext;

	public FakerItemReader() {
		setName(ClassUtils.getShortName(getClass()));
	}

	public void setLocale(Locale locale) {
		this.locale = locale;
	}

	public void setFields(Map<String, Expression> fields) {
		this.fields = fields;
	}

	public void setStringFields(Map<String, String> stringFields) {
		Map<String, Expression> expressions = new LinkedHashMap<>();
		stringFields.forEach((k, v) -> expressions.put(k, RiotUtils.parse(v)));
		this.fields = expressions;
	}

	@Override
	protected synchronized void doOpen() throws Exception {
		if (evaluationContext == null) {
			Assert.notEmpty(fields, "No field specified");
			evaluationContext = new StandardEvaluationContext();
			evaluationContext.addPropertyAccessor(new ReflectivePropertyAccessor());
			evaluationContext.addMethodResolver(DataBindingMethodResolver.forInstanceMethodInvocation());
			evaluationContext.setRootObject(new AugmentedFaker(locale));
		}
	}

	@Override
	protected Map<String, Object> doRead() throws Exception {
		Map<String, Object> map = new HashMap<>();
		fields.forEach((k, v) -> map.put(k, v.getValue(evaluationContext)));
		return map;
	}

	@Override
	protected synchronized void doClose() {
		evaluationContext = null;
	}

	public class AugmentedFaker extends Faker {

		public AugmentedFaker(Locale locale) {
			super(locale);
		}

		public int getIndex() {
			return index();
		}

		public int index() {
			return getCurrentItemCount();
		}

		public long getThread() {
			return thread();
		}

		@SuppressWarnings("deprecation")
		public long thread() {
			return Thread.currentThread().getId();
		}

	}

}
