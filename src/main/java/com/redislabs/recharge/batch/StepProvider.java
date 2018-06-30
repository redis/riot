package com.redislabs.recharge.batch;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;

import com.redislabs.recharge.redis.HashItem;

public interface StepProvider {

	AbstractItemCountingItemStreamItemReader<Map<String, String>> getReader() throws Exception;

	ItemProcessor<Map<String, String>, HashItem> getProcessor();

}
