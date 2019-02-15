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


}