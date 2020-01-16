package com.redislabs.riot.transfer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@SuppressWarnings("rawtypes")
@Accessors(fluent = true)
public class Flow {

	private @Getter String name;
	private @Getter ItemReader reader;
	private ItemProcessor processor;
	private @Getter ItemWriter writer;
	private @Getter @Setter int nThreads;
	private @Getter @Setter int batchSize;
	private @Getter @Setter Long flushRate;

	@Builder
	private Flow(String name, ItemReader reader, ItemProcessor processor, ItemWriter writer, int nThreads,
			int batchSize, Long flushRate) {
		this.name = name;
		this.reader = reader;
		this.processor = processor;
		this.writer = writer;
		this.nThreads = nThreads;
		this.batchSize = batchSize;
		this.flushRate = flushRate;
	}

	public FlowExecution execute() {
		List<FlowThread> threads = new ArrayList<>(nThreads);
		for (int index = 0; index < nThreads; index++) {
			Batcher batcher = Batcher.builder().reader(reader).processor(processor()).chunkSize(batchSize).build();
			FlowThread thread = FlowThread.builder().threadId(index).threads(nThreads).flow(this).batcher(batcher)
					.flushRate(flushRate).build();
			threads.add(thread);
		}
		ExecutorService executor = Executors.newFixedThreadPool(threads.size());
		for (FlowThread thread : threads) {
			executor.submit(thread);
		}
		executor.shutdown();
		return FlowExecution.builder().threads(threads).executor(executor).build();
	}

	private ItemProcessor processor() {
		if (processor == null) {
			return item -> item;
		}
		return processor;
	}

}
