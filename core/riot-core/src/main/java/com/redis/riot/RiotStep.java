package com.redis.riot;

import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

public class RiotStep<I, O> {

	private String name;
	private String taskName;
	private ItemReader<I> reader;
	private Optional<ItemProcessor<I, O>> processor = Optional.empty();
	private ItemWriter<O> writer;
	private Optional<Supplier<String>> extraMessage = Optional.empty();
	private Optional<Supplier<Long>> initialMax = Optional.empty();

	private RiotStep(Builder<I, O> builder) {
		this.name = builder.name;
		this.taskName = builder.taskName;
		this.reader = builder.reader;
		this.processor = builder.processor;
		this.writer = builder.writer;
		this.extraMessage = builder.extraMessage;
		this.initialMax = builder.initialMax;
	}

	public String getName() {
		return name;
	}

	public String getTaskName() {
		return taskName;
	}

	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}

	public ItemReader<I> getReader() {
		return reader;
	}

	public void setReader(ItemReader<I> reader) {
		this.reader = reader;
	}

	public Optional<ItemProcessor<I, O>> getProcessor() {
		return processor;
	}

	public ItemWriter<O> getWriter() {
		return writer;
	}

	public void setWriter(ItemWriter<O> writer) {
		this.writer = writer;
	}

	public Optional<Supplier<String>> getExtraMessage() {
		return extraMessage;
	}

	public void setExtraMessage(Optional<Supplier<String>> extraMessage) {
		this.extraMessage = extraMessage;
	}

	public Optional<Supplier<Long>> getInitialMax() {
		return initialMax;
	}

	public void setInitialMax(Supplier<Long> initialMax) {
		this.initialMax = Optional.of(initialMax);
	}

	public void setInitialMax(Optional<Supplier<Long>> initialMax) {
		this.initialMax = initialMax;
	}

	public static <I, O> Builder<I, O> builder() {
		return new Builder<>();
	}

	public static final class Builder<I, O> {
		private String name;
		private String taskName;
		private ItemReader<I> reader;
		private Optional<ItemProcessor<I, O>> processor = Optional.empty();
		private ItemWriter<O> writer;
		private Optional<Supplier<String>> extraMessage = Optional.empty();
		private Optional<Supplier<Long>> initialMax = Optional.empty();

		private Builder() {
		}

		public Builder<I, O> name(String name) {
			this.name = name;
			return this;
		}

		public Builder<I, O> taskName(String taskName) {
			this.taskName = taskName;
			return this;
		}

		public Builder<I, O> reader(ItemReader<I> reader) {
			this.reader = reader;
			return this;
		}

		public Builder<I, O> processor(Optional<ItemProcessor<I, O>> processor) {
			this.processor = processor;
			return this;
		}

		public Builder<I, O> processor(ItemProcessor<I, O> processor) {
			Assert.notNull(processor, "Processor must not be null");
			this.processor = Optional.of(processor);
			return this;
		}

		public Builder<I, O> writer(ItemWriter<O> writer) {
			this.writer = writer;
			return this;
		}

		public Builder<I, O> extraMessage(Supplier<String> extraMessage) {
			Assert.notNull(extraMessage, "Extra message supplier must not be null");
			this.extraMessage = Optional.of(extraMessage);
			return this;
		}

		public Builder<I, O> initialMax(Supplier<Long> initialMax) {
			Assert.notNull(initialMax, "Initial max must not be null");
			this.initialMax = Optional.of(initialMax);
			return this;
		}

		public RiotStep<I, O> build() {
			return new RiotStep<>(this);
		}
	}

}
