package com.redislabs.recharge.generator;

import lombok.Data;

@Data
public class GeneratorReaderContext {
	private int partitions;
	private int partitionIndex;
	private long count;

	public void incrementCount() {
		count++;
	}

	public long nextLong(long end) {
		return nextLong(0, end);
	}

	public long nextLong(long start, long end) {
		long segment = (end - start) / partitions;
		long partitionStart = start + partitionIndex * segment;
		return partitionStart + (count % segment);
	}

	public String nextId(long start, long end, String format) {
		return String.format(format, nextLong(start, end));
	}

	public String nextId(long end, String format) {
		return nextId(0, end, format);
	}
}