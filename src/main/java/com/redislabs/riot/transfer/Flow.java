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

@Builder
public class Flow<I, O> {

	private @Getter String name;
	private @Getter ItemReader<I> reader;
	private ItemProcessor<I, O> processor;
	private @Getter ItemWriter<O> writer;
	private int nThreads;
	private int batchSize;
	private @Setter Long flushRate;
	private ErrorHandler errorHandler;

	public FlowExecution<I, O> execute() {
		List<FlowThread<I, O>> flowThreads = new ArrayList<>(nThreads);
		for (int index = 0; index < nThreads; index++) {
			Batcher<I, O> batcher = Batcher.<I, O>builder().reader(reader).processor(processor).batchSize(batchSize)
					.errorHandler(errorHandler).build();
			flowThreads.add(FlowThread.<I, O>builder().flow(this).threadId(index).threads(nThreads).batcher(batcher)
					.flushRate(flushRate).build());
		}
		ExecutorService executor = Executors.newFixedThreadPool(flowThreads.size());
		for (FlowThread<I, O> thread : flowThreads) {
			executor.submit(thread);
		}
		executor.shutdown();
		return FlowExecution.<I, O>builder().threads(flowThreads).executor(executor).build();
	}

}
