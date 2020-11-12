package com.redislabs.riot;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader.RedisDataStructureItemReaderBuilder;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.Transfer;

import picocli.CommandLine.Mixin;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure, O> {

    @Mixin
    private RedisExportOptions options = new RedisExportOptions();

    @Override
    protected List<Transfer<DataStructure, O>> transfers() throws Exception {
	RedisDataStructureItemReaderBuilder builder = configure(
		RedisDataStructureItemReader.builder().batch(options.getReaderBatchSize())
			.queueCapacity(options.getQueueCapacity()).threads(options.getReaderThreads())
			.scanCount(options.getScanCount()).scanMatch(options.getScanMatch()));
	RedisDataStructureItemReader reader = builder.build();
	reader.setName(toString(builder.uri()));
	return Collections.singletonList(transfer(reader, processor(), writer()));
    }

    protected abstract ItemProcessor<DataStructure, O> processor();

    protected abstract ItemWriter<O> writer() throws Exception;

    @Override
    protected String transferNameFormat() {
	return "Exporting from %s";
    }

}
