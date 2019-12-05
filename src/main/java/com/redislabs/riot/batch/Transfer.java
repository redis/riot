package com.redislabs.riot.batch;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

public interface Transfer<I, O> {

	ItemReader<I> reader(TransferContext context) throws Exception;

	ItemProcessor<I, O> processor(TransferContext context) throws Exception;

	ItemWriter<O> writer(TransferContext context) throws Exception;

}
