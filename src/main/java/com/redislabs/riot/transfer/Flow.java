package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Builder;
import lombok.Data;

@SuppressWarnings("rawtypes")
@Builder
public @Data class Flow {

	private String name;
	private ItemReader reader;
	private ItemProcessor processor;
	private ItemWriter writer;
	private int nThreads;
	private int batchSize;
	private Long flushRate;
	private ErrorHandler errorHandler;

	public FlowExecution execute() {
		List<FlowThread> threads = new ArrayList<>(nThreads);
		for (int index = 0; index < nThreads; index++) {
			Batcher batcher = new Batcher().reader(reader).processor(createProcessor()).chunkSize(batchSize)
					.queueCapacity(batchSize * 2).errorHandler(errorHandler);
			FlowThread thread = new FlowThread().threadId(index).threads(nThreads).flow(this).batcher(batcher)
					.flushRate(flushRate);
			threads.add(thread);
		}
		ExecutorService executor = Executors.newFixedThreadPool(threads.size());
		for (FlowThread thread : threads) {
			executor.submit(thread);
		}
		executor.shutdown();
		return new FlowExecution().threads(threads).executor(executor);
	}

	private ItemProcessor createProcessor() {
		if (processor == null) {
			return item -> item;
		}
		return processor;
	}

}
