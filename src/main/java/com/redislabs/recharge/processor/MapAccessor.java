package com.redislabs.recharge.processor;

import java.util.Map;

import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.PropertyAccessor;
import org.springframework.expression.TypedValue;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class MapAccessor implements PropertyAccessor {

	public boolean canRead(EvaluationContext context, Object target, String name) throws AccessException {
		Map map = (Map) target;
		return map.containsKey(name);
	}

	public TypedValue read(EvaluationContext context, Object target, String name) throws AccessException {
		Map map = (Map) target;
		Object value = map.get(name);
		return new TypedValue(value);
	}

	public boolean canWrite(EvaluationContext context, Object target, String name) throws AccessException {
		return true;
	}

	public void write(EvaluationContext context, Object target, String name, Object newValue) throws AccessException {
		Map map = (Map) target;
		map.put(name, newValue);
	}

	public Class[] getSpecificTargetClasses() {
		return new Class[] { Map.class };
	}

}