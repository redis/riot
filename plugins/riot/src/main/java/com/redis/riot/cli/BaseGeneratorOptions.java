package com.redis.riot.cli;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import picocli.CommandLine.Option;

public class BaseGeneratorOptions {

	@Option(names = "--start", description = "Start index (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int start = 1;
	@Option(names = "--count", description = "Number of items to generate (default: ${DEFAULT-VALUE}).", paramLabel = "<int>")
	protected int count = 1000;

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

	public ProgressMonitor.Builder configure(ProgressMonitor.Builder monitor) {
		return monitor.initialMax(() -> (long) count);
	}

	@Override
	public String toString() {
		return "GeneratorOptions [start=" + start + ", count=" + count + "]";
	}

}
