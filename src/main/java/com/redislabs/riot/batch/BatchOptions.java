package com.redislabs.riot.batch;

import lombok.Data;

@Data
public class BatchOptions {

	private int partitions;
	private int chunkSize;
	private long sleep;

}
