package com.redislabs.riot;

import java.util.Collections;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemReader;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.redis.support.Transfer;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulConnection;
import picocli.CommandLine.Mixin;

public abstract class AbstractExportCommand<O> extends AbstractTransferCommand<DataStructure, O> {

	@Mixin
	private RedisExportOptions options = new RedisExportOptions();

	@Override
	protected List<Transfer<DataStructure, O>> transfers(RedisURI uri, AbstractRedisClient client,
			GenericObjectPoolConfig<StatefulConnection<String, String>> poolConfig) throws Exception {
		DataStructureItemReader reader = DataStructureItemReader.builder().client(client).poolConfig(poolConfig)
				.options(options.readerOptions()).build();
		reader.setName(toString(uri));
		return Collections.singletonList(transfer(reader, processor(), writer()));
	}

	protected abstract ItemProcessor<DataStructure, O> processor();

	protected abstract ItemWriter<O> writer() throws Exception;

	@Override
	protected String transferNameFormat() {
		return "Exporting from %s";
	}

}
