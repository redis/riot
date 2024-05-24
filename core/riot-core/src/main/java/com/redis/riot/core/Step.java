package com.redis.riot.core;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

public class Step<I, O> {

	private static final long NO_MAX_ITEM_COUNT = -1;
	private static final String EMPTY_STRING = "";

	private String name;
	private String taskName;
	private Supplier<String> statusMessageSupplier = () -> EMPTY_STRING;
	private LongSupplier maxItemCountSupplier = () -> NO_MAX_ITEM_COUNT;
	private ItemReader<? extends I> reader;
	private ItemProcessor<? super I, ? extends O> processor;
	private ItemWriter<? super O> writer;
	private Set<StepExecutionListener> executionListeners = new LinkedHashSet<>();
	private Set<ItemWriteListener<? super O>> writeListeners = new LinkedHashSet<>();
	private Duration flushInterval;
	private Duration idleTimeout;

	public Step(ItemReader<? extends I> reader, ItemWriter<? super O> writer) {
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

	public ItemReader<? extends I> getReader() {
		return reader;
	}

	public ItemProcessor<? super I, ? extends O> getProcessor() {
		return processor;
	}

	public ItemWriter<? super O> getWriter() {
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

	public Step<I, O> reader(ItemReader<? extends I> reader) {
		this.reader = reader;
		return this;
	}

	public Step<I, O> writer(ItemWriter<? super O> writer) {
		this.writer = writer;
		return this;
	}

	public Step<I, O> processor(ItemProcessor<? super I, ? extends O> processor) {
		this.processor = processor;
		return this;
	}

	public Step<I, O> maxItemCount(int count) {
		return maxItemCountSupplier(() -> count);
	}

	public Step<I, O> writeListener(ItemWriteListener<? super O> listener) {
		writeListeners.add(listener);
		return this;
	}

	public Set<ItemWriteListener<? super O>> getWriteListeners() {
		return writeListeners;
	}

	public Step<I, O> executionListener(StepExecutionListener listener) {
		executionListeners.add(listener);
		return this;
	}

	public Set<StepExecutionListener> getExecutionListeners() {
		return executionListeners;
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

}
