package com.redis.riot;

import java.util.Optional;
import java.util.function.Supplier;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.util.Assert;

public class RiotStep<I, O> {

	private final String taskName;
	private final ItemReader<I> reader;
	private final ItemProcessor<I, O> processor;
	private final ItemWriter<O> writer;
	private final Optional<Supplier<String>> message;
	private final Optional<Supplier<Long>> max;

	private RiotStep(Builder<I, O> builder) {
		this.taskName = builder.taskName;
		this.reader = builder.reader;
		this.processor = builder.processor;
		this.writer = builder.writer;
		this.message = builder.message;
		this.max = builder.max;
	}

	public String getTaskName() {
		return taskName;
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

	public Optional<Supplier<String>> getMessage() {
		return message;
	}

	public Optional<Supplier<Long>> getMax() {
		return max;
	}

	public static <I> WriterBuilder<I> reader(ItemReader<I> reader) {
		return new WriterBuilder<>(reader);
	}

	public static class WriterBuilder<I> {
		private final ItemReader<I> reader;

		public WriterBuilder(ItemReader<I> reader) {
			this.reader = reader;
		}

		public <O> Builder<I, O> writer(ItemWriter<O> writer) {
			return new Builder<>(reader, writer);
		}
	}

	public static final class Builder<I, O> {
		private final ItemReader<I> reader;
		private final ItemWriter<O> writer;
		private String taskName;
		private ItemProcessor<I, O> processor;
		private Optional<Supplier<String>> message = Optional.empty();
		private Optional<Supplier<Long>> max = Optional.empty();

		public Builder(ItemReader<I> reader, ItemWriter<O> writer) {
			this.reader = reader;
			this.writer = writer;
		}

		public Builder<I, O> taskName(String taskName) {
			this.taskName = taskName;
			return this;
		}

		public Builder<I, O> processor(ItemProcessor<I, O> processor) {
			this.processor = processor;
			return this;
		}

		public Builder<I, O> message(Supplier<String> message) {
			Assert.notNull(message, "Extra message supplier must not be null");
			this.message = Optional.of(message);
			return this;
		}

		public Builder<I, O> max(Supplier<Long> max) {
			Assert.notNull(max, "Initial max must not be null");
			this.max = Optional.of(max);
			return this;
		}

		public RiotStep<I, O> build() {
			return new RiotStep<>(this);
		}
	}

}
