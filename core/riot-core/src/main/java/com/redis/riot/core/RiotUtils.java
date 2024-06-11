package com.redis.riot.core;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.Expression;
import org.springframework.expression.common.TemplateParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

public abstract class RiotUtils {

	private static final SpelExpressionParser parser = new SpelExpressionParser();

	private RiotUtils() {
	}

	public static Expression parse(String expressionString) {
		return parser.parseExpression(expressionString);
	}

	public static TemplateExpression parseTemplate(String expressionString) {
		TemplateExpression expression = new TemplateExpression();
		expression.setExpression(parser.parseExpression(expressionString, new TemplateParserContext()));
		return expression;
	}

	public static <T> Predicate<T> predicate(EvaluationContext context, Expression expression) {
		return t -> expression.getValue(context, t, Boolean.class);
	}

	public static <S, T> ItemProcessor<S, T> processor(Collection<? extends Function<?, ?>> functions) {
		return processor(functions.toArray(new Function[0]));
	}

	@SuppressWarnings("unchecked")
	public static <S, T> ItemProcessor<S, T> processor(Function<?, ?>... functions) {
		return processor(Stream.of(functions).filter(Objects::nonNull).map(FunctionItemProcessor::new)
				.toArray(ItemProcessor[]::new));
	}

	@SuppressWarnings("unchecked")
	public static <S, T> ItemProcessor<S, T> processor(ItemProcessor<?, ?>... processors) {
		List<? extends ItemProcessor<?, ?>> list = Stream.of(processors).filter(Objects::nonNull)
				.collect(Collectors.toList());
		if (list.isEmpty()) {
			return null;
		}
		if (list.size() > 1) {
			CompositeItemProcessor<S, T> composite = new CompositeItemProcessor<>();
			composite.setDelegates(list);
			return composite;
		}
		return (ItemProcessor<S, T>) list.get(0);
	}

	@SuppressWarnings("unchecked")
	public static <T> ItemWriter<T> writer(ItemWriter<T>... writers) {
		return writer(Arrays.asList(writers));
	}

	public static <T> ItemWriter<T> writer(Collection<? extends ItemWriter<T>> writers) {
		if (writers.isEmpty()) {
			throw new IllegalArgumentException("At least one writer must be specified");
		}
		if (writers.size() == 1) {
			return writers.iterator().next();
		}
		CompositeItemWriter<T> composite = new CompositeItemWriter<>();
		composite.setDelegates(new ArrayList<>(writers));
		return composite;
	}

	public static boolean isPositive(Duration duration) {
		return duration != null && !duration.isNegative() && !duration.isZero();
	}

	public static PrintStream newPrintStream(OutputStream out) {
		return newPrintStream(out, true);
	}

	public static PrintStream newPrintStream(OutputStream out, boolean autoFlush) {
		try {
			return new PrintStream(out, autoFlush, UTF_8.name());
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static PrintWriter newPrintWriter(OutputStream out) {
		return newPrintWriter(out, true);
	}

	public static PrintWriter newPrintWriter(OutputStream out, boolean autoFlush) {
		return new PrintWriter(new BufferedWriter(new OutputStreamWriter(out, UTF_8)), autoFlush);
	}

	public static String toString(ByteArrayOutputStream out) {
		try {
			return out.toString(UTF_8.name());
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static String toString(Expression expression) {
		if (expression == null) {
			return String.valueOf(expression);
		}
		return expression.getExpressionString();
	}

}
