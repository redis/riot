package com.redis.riot.cli.gen;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import picocli.CommandLine.Option;

public class GeneratorOptions {

	public static final int DEFAULT_START = 1;
	public static final int DEFAULT_COUNT = 1000;

	@Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int start = DEFAULT_START;

	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int count = DEFAULT_COUNT;

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public void configure(AbstractItemCountingItemStreamItemReader<?> reader) {
		reader.setCurrentItemCount(start - 1);
		reader.setMaxItemCount(count);
	}

	@Override
	public String toString() {
		return "GeneratorOptions [start=" + start + ", count=" + count + "]";
	}

}
