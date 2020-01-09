package com.redislabs.riot;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import lombok.Data;

public @Data class Transfer<I, O> {

	private ItemReader<I> reader;
	private ItemProcessor<I, O> processor;
	private ItemWriter<O> writer;
	private int nThreads;
	private int batchSize;

	public TransferExecution<I, O> execute() {
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		List<TransferThread<I, O>> threads = new ArrayList<>(nThreads);
		for (int index = 0; index < nThreads; index++) {
			threads.add(new TransferThread<>(index, reader, processor, writer, this));
		}
		for (TransferThread<I, O> thread : threads) {
			executor.execute(thread);
		}
		executor.shutdown();
		return new TransferExecution<>(threads, executor);
	}

}
