package com.redis.riot.cli.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.riot.cli.common.JobOptions.ProgressStyle;
import com.redis.riot.core.FakerItemReader;
import com.redis.riot.core.ThrottledItemWriter;
import com.redis.riot.core.logging.RiotLevel;
import com.redis.spring.batch.common.Utils;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisCommandTimeoutException;
import me.tongfei.progressbar.DelegatingProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarStyle;

public class RiotStep<I, O> {

	private static final Logger log = Logger.getLogger(RiotStep.class.getName());

	@SuppressWarnings("rawtypes")
	private static final Class[] DEFAULT_SKIPPABLE_EXCEPTIONS = { RedisCommandExecutionException.class,
			RedisCommandTimeoutException.class, TimeoutException.class };
	private final StepBuilderFactory factory;
	private JobOptions options = new JobOptions();
	private final ItemReader<I> reader;
	private final ItemWriter<O> writer;
	private String name;
	private String task;
	private Supplier<String> extraMessage;
	private ItemProcessor<I, O> processor;
	@SuppressWarnings("rawtypes")
	private List<Class> skippableExceptions = new ArrayList<>(Arrays.asList(DEFAULT_SKIPPABLE_EXCEPTIONS));
	private Consumer<String> logger = s -> log.log(RiotLevel.LIFECYCLE, s);
	private Optional<ProgressStyle> progressStyle = Optional.empty();

	public RiotStep(StepBuilderFactory factory, ItemReader<I> reader, ItemWriter<O> writer) {
		this.factory = factory;
		this.reader = reader;
		this.writer = writer;
	}

	public RiotStep<I, O> progressStyle(ProgressStyle style) {
		this.progressStyle = Optional.of(style);
		return this;
	}

	public RiotStep<I, O> name(String name) {
		this.name = name;
		return this;
	}

	public RiotStep<I, O> options(JobOptions options) {
		this.options = options;
		return this;
	}

	public RiotStep<I, O> task(String task) {
		this.task = task;
		return this;
	}

	public RiotStep<I, O> logger(Consumer<String> logger) {
		this.logger = logger;
		return this;
	}

	public RiotStep<I, O> extraMessage(Supplier<String> extraMessage) {
		this.extraMessage = extraMessage;
		return this;
	}

	public RiotStep<I, O> processor(ItemProcessor<I, O> processor) {
		this.processor = processor;
		return this;
	}

	@SuppressWarnings("rawtypes")
	public RiotStep<I, O> skippableExceptions(Class... classes) {
		this.skippableExceptions.addAll(Arrays.asList(classes));
		return this;
	}

	public Supplier<String> getExtraMessage() {
		return extraMessage;
	}

	public String getName() {
		return name;
	}

	public ItemProcessor<I, O> getProcessor() {
		return processor;
	}

	public ItemReader<I> getReader() {
		return reader;
	}

	public String getTask() {
		return task;
	}

	public ItemWriter<O> getWriter() {
		return writer;
	}

	public SimpleStepBuilder<I, O> build() {
		SimpleStepBuilder<I, O> step = factory.get(name).chunk(options.getChunkSize());
		step.reader(reader);
		step.processor(processor);
		step.writer(writer());
		Utils.multiThread(step, options.getThreads());
		if (progressStyle() != ProgressStyle.NONE) {
			step.listener((StepExecutionListener) stepListener());
		}
		if (options.getSkipPolicy() == StepSkipPolicy.NEVER) {
			return step;
		}
		return step.faultTolerant().skipPolicy(skipPolicy());
	}

	private ProgressStepListener<O> stepListener() {
		ProgressBarBuilder pbb = new ProgressBarBuilder();
		pbb.setInitialMax(initialMax());
		pbb.setTaskName(task);
		pbb.setStyle(progressBarStyle());
		pbb.setUpdateIntervalMillis(options.getProgressUpdateInterval());
		pbb.showSpeed();
		if (progressStyle() == ProgressStyle.LOG) {
			pbb.setConsumer(new DelegatingProgressBarConsumer(logger));
		}
		if (extraMessage == null) {
			return new ProgressStepListener<>(pbb);
		}
		return new ProgressWithExtraMessageStepListener<>(pbb, extraMessage);
	}

	private ProgressStyle progressStyle() {
		return progressStyle.orElse(options.getProgressStyle());
	}

	private long initialMax() {
		if (reader instanceof FakerItemReader) {
			return ((FakerItemReader) reader).size();
		}
		return Utils.getItemReaderSize(reader);
	}

	private ProgressBarStyle progressBarStyle() {
		switch (progressStyle()) {
		case BAR:
			return ProgressBarStyle.COLORFUL_UNICODE_BAR;
		case BLOCK:
			return ProgressBarStyle.COLORFUL_UNICODE_BLOCK;
		default:
			return ProgressBarStyle.ASCII;
		}
	}

	private ItemWriter<O> writer() {
		Duration sleep = Duration.ofMillis(options.getSleep());
		if (sleep.isNegative() || sleep.isZero()) {
			return writer;
		}
		return new ThrottledItemWriter<>(writer, sleep);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private SkipPolicy skipPolicy() {
		switch (options.getSkipPolicy()) {
		case ALWAYS:
			return new AlwaysSkipItemSkipPolicy();
		case NEVER:
			return new NeverSkipItemSkipPolicy();
		default:
			return new LimitCheckingItemSkipPolicy(options.getSkipLimit(), (Map) skippableExceptions());
		}
	}

	@SuppressWarnings("rawtypes")
	private Map<Class, Boolean> skippableExceptions() {
		return skippableExceptions.stream().collect(Collectors.toMap(Function.identity(), t -> true));
	}

}
