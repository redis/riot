package com.redis.riot.core;

import org.springframework.context.expression.MapAccessor;
import org.springframework.expression.AccessException;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.TypedValue;
import org.springframework.lang.Nullable;

/**
 * {@link org.springframework.context.expression.MapAccessor} that always
 * returns true for canRead and does not throw AccessExceptions
 */
public class QuietMapAccessor extends MapAccessor {

	@Override
	public boolean canRead(EvaluationContext context, @Nullable Object target, String name) {
		return true;
	}

	@Override
	public TypedValue read(EvaluationContext context, @Nullable Object target, String name) {
		try {
			return super.read(context, target, name);
		} catch (AccessException e) {
			return new TypedValue(null);
		}
	}

}