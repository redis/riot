package com.redislabs.riot;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader;
import org.springframework.batch.item.redis.RedisDataStructureItemReader.RedisDataStructureItemReaderBuilder;
import org.springframework.batch.item.redis.support.DataStructure;

import picocli.CommandLine.Mixin;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure<String>, O> {

    @Mixin
    private RedisExportOptions options = new RedisExportOptions();

    @Override
    protected String taskName() {
	return "Exporting from";
    }

    @Override
    protected List<ItemReader<DataStructure<String>>> readers() throws Exception {
	RedisDataStructureItemReaderBuilder<String, String> builder = configure(RedisDataStructureItemReader.builder()
		.scanCount(options.getScanCount()).scanMatch(options.getScanMatch()).batch(options.getReaderBatchSize())
		.queueCapacity(options.getQueueCapacity()).threads(options.getReaderThreads()));
	RedisDataStructureItemReader<String, String> reader = builder.build();
	reader.setName(toString(builder.uri()));
	return Collections.singletonList(reader);
    }

}
