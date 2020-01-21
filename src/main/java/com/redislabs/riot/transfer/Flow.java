package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Getter;
import lombok.Setter;

@SuppressWarnings("rawtypes")
public class Flow {

	private @Getter @Setter String name;
	private @Getter @Setter ItemReader reader;
	private @Setter ItemProcessor processor;
	private @Getter @Setter ItemWriter writer;
	private @Getter @Setter int nThreads;
	private @Getter @Setter int batchSize;
	private @Getter @Setter Long flushRate;

	public FlowExecution execute() {
		List<FlowThread> threads = new ArrayList<>(nThreads);
		for (int index = 0; index < nThreads; index++) {
			Batcher batcher = new Batcher().reader(reader).processor(processor()).chunkSize(batchSize)
					.queueCapacity(batchSize * 2);
			;
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

	private ItemProcessor processor() {
		if (processor == null) {
			return item -> item;
		}
		return processor;
	}

}
