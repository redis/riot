package com.redislabs.riot.generator;

import java.util.Locale;
import java.util.Map;

import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.SimpleEvaluationContext.Builder;

public class FakerGeneratorReader extends GeneratorReader {

	private Locale locale;
	private Map<String, Expression> fieldExpressions;
	private ThreadLocal<EvaluationContext> context = new ThreadLocal<>();

	public FakerGeneratorReader(Locale locale, Map<String, Expression> fieldExpressions) {
		this.locale = locale;
		this.fieldExpressions = fieldExpressions;
	}

	@Override
	protected void doOpen() throws Exception {
		ReflectivePropertyAccessor accessor = new ReflectivePropertyAccessor();
		GeneratorFaker faker = new GeneratorFaker(locale, this);
		context.set(new Builder(accessor).withInstanceMethods().withRootObject(faker).build());
		super.doOpen();
	}

	@Override
	protected void generate(Map<String, Object> map) {
		fieldExpressions.forEach((k, v) -> map.put(k, v.getValue(context.get())));
	}

	@Override
	protected void doClose() throws Exception {
		context.remove();
		super.doClose();
	}

	@Override
	public String toString() {
		return "Faker generator";
	}

}
