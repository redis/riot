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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.ClassUtils;

public abstract class RiotUtils {

	private RiotUtils() {
	}

	public static <S, T> ItemProcessor<S, T> processor(Collection<? extends Function<?, ?>> functions) {
		return processor(functions.toArray(new Function[0]));
	}

	@SuppressWarnings("unchecked")
	public static <S, T> ItemProcessor<S, T> processor(Function<?, ?>... functions) {
		return processor(Stream.of(functions).filter(Objects::nonNull).map(FunctionItemProcessor::new)
				.toArray(ItemProcessor[]::new));
	}

	public static <S, T> ItemProcessor<S, T> processor(ItemProcessor<?, ?>... processors) {
		return processor(Stream.of(processors));
	}

	public static <S, T> ItemProcessor<S, T> processor(Iterable<? extends ItemProcessor<?, ?>> processors) {
		return processor(StreamSupport.stream(processors.spliterator(), false));
	}

	@SuppressWarnings("unchecked")
	public static <S, T> ItemProcessor<S, T> processor(Stream<? extends ItemProcessor<?, ?>> processors) {
		List<? extends ItemProcessor<?, ?>> list = processors.filter(Objects::nonNull).collect(Collectors.toList());
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

	public static <T> ItemWriter<T> writer(Stream<? extends ItemWriter<T>> writers) {
		return writer(writers.collect(Collectors.toList()));
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

	public static void registerFunction(StandardEvaluationContext context, String functionName, Class<?> clazz,
			String methodName, Class<?>... parameterTypes) {
		try {
			context.registerFunction(functionName, clazz.getDeclaredMethod(methodName, parameterTypes));
		} catch (Exception e) {
			throw new UnsupportedOperationException(
					String.format("Could not get method %s.%s", ClassUtils.getQualifiedName(clazz), methodName), e);
		}
	}

}
