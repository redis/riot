package com.redislabs.riot;

import java.util.Collections;
import java.util.List;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.Transfer;

import picocli.CommandLine.Mixin;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure, O> {

	@Mixin
	private RedisExportOptions options = new RedisExportOptions();

	@Override
	protected List<Transfer<DataStructure, O>> transfers(TransferContext context) throws Exception {
		DataStructureItemReader reader = DataStructureItemReader.builder(context.getClient())
				.poolConfig(context.getRedisOptions().poolConfig()).options(options.readerOptions()).build();
		reader.setName(toString(context.getRedisOptions().uri()));
		return Collections.singletonList(transfer(reader, processor(), writer()));
	}

	protected abstract ItemProcessor<DataStructure, O> processor();

	protected abstract ItemWriter<O> writer() throws Exception;

	@Override
	protected String transferNameFormat() {
		return "Exporting from %s";
	}

}
