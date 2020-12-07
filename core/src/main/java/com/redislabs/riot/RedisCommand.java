package com.redislabs.riot;

import org.springframework.batch.item.ItemWriter;

public interface RedisCommand<T> {

	ItemWriter<T> writer(TransferContext context) throws Exception;
}
