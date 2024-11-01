package com.redis.riot.core;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.ItemReadListener;
import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import com.redis.spring.batch.step.FlushingChunkProvider;

import lombok.ToString;

@ToString
public class Step<I, O> {

	private static final long NO_MAX_ITEM_COUNT = -1;
	private static final String EMPTY_STRING = "";
	public static final String DEFAULT_NAME = "step";

	private String name = DEFAULT_NAME;
	private final ItemReader<I> reader;
	private final ItemWriter<O> writer;
	private String taskName;
	private Supplier<String> statusMessageSupplier = () -> EMPTY_STRING;
	private LongSupplier maxItemCountSupplier = () -> NO_MAX_ITEM_COUNT;
	private ItemProcessor<I, O> processor;
	private Set<StepExecutionListener> executionListeners = new LinkedHashSet<>();
	private Set<ItemReadListener<I>> readListeners = new LinkedHashSet<>();
	private Set<ItemWriteListener<O>> writeListeners = new LinkedHashSet<>();
	private boolean live;
	private Duration flushInterval = FlushingChunkProvider.DEFAULT_FLUSH_INTERVAL;
	private Duration idleTimeout = FlushingChunkProvider.DEFAULT_IDLE_TIMEOUT;
	private Collection<Class<? extends Throwable>> skip = new HashSet<>();
	private Collection<Class<? extends Throwable>> noSkip = new HashSet<>();
	private Collection<Class<? extends Throwable>> retry = new HashSet<>();
	private Collection<Class<? extends Throwable>> noRetry = new HashSet<>();

	public Step(ItemReader<I> reader, ItemWriter<O> writer) {
		this.reader = reader;
		this.writer = writer;
	}

	public String getName() {
		return name;
	}

	public Step<I, O> name(String name) {
		this.name = name;
		return this;
	}

	public String getTaskName() {
		return taskName;
	}

	public Step<I, O> taskName(String name) {
		this.taskName = name;
		return this;
	}

	public ItemReader<I> getReader() {
		return reader;
	}

	public ItemProcessor<I, O> getProcessor() {
		return processor;
	}

	public ItemWriter<O> getWriter() {
		return writer;
	}

	public long maxItemCount() {
		return maxItemCountSupplier.getAsLong();
	}

	public String statusMessage() {
		return statusMessageSupplier.get();
	}

	public Supplier<String> getStatusMessageSupplier() {
		return statusMessageSupplier;
	}

	public Step<I, O> statusMessageSupplier(Supplier<String> supplier) {
		this.statusMessageSupplier = supplier;
		return this;
	}

	public LongSupplier getMaxItemCountSupplier() {
		return maxItemCountSupplier;
	}

	public Step<I, O> maxItemCountSupplier(LongSupplier supplier) {
		this.maxItemCountSupplier = supplier;
		return this;
	}

	public Step<I, O> processor(ItemProcessor<I, O> processor) {
		this.processor = processor;
		return this;
	}

	public Step<I, O> maxItemCount(int count) {
		return maxItemCountSupplier(() -> count);
	}

	public Set<ItemReadListener<I>> getReadListeners() {
		return readListeners;
	}

	public Step<I, O> readListener(ItemReadListener<I> listener) {
		this.readListeners.add(listener);
		return this;
	}

	public Set<ItemWriteListener<O>> getWriteListeners() {
		return writeListeners;
	}

	public Step<I, O> writeListener(ItemWriteListener<O> listener) {
		writeListeners.add(listener);
		return this;
	}

	public Set<StepExecutionListener> getExecutionListeners() {
		return executionListeners;
	}

	public Step<I, O> executionListener(StepExecutionListener listener) {
		executionListeners.add(listener);
		return this;
	}

	public Duration getFlushInterval() {
		return flushInterval;
	}

	public Step<I, O> flushInterval(Duration flushInterval) {
		this.flushInterval = flushInterval;
		return this;
	}

	public Duration getIdleTimeout() {
		return idleTimeout;
	}

	public Step<I, O> idleTimeout(Duration idleTimeout) {
		this.idleTimeout = idleTimeout;
		return this;
	}

	public boolean isLive() {
		return live;
	}

	public Step<I, O> live(boolean live) {
		this.live = live;
		return this;
	}

	public Step<I, O> skip(Class<? extends Throwable> exception) {
		skip.add(exception);
		return this;
	}

	public Step<I, O> retry(Class<? extends Throwable> exception) {
		retry.add(exception);
		return this;
	}

	public Step<I, O> noSkip(Class<? extends Throwable> exception) {
		noSkip.add(exception);
		return this;
	}

	public Step<I, O> noRetry(Class<? extends Throwable> exception) {
		noRetry.add(exception);
		return this;
	}

	public Collection<Class<? extends Throwable>> getNoRetry() {
		return noRetry;
	}

	public Collection<Class<? extends Throwable>> getNoSkip() {
		return noSkip;
	}

	public Collection<Class<? extends Throwable>> getRetry() {
		return retry;
	}

	public Collection<Class<? extends Throwable>> getSkip() {
		return skip;
	}

}
