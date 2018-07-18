package com.redislabs.recharge.redis;

import java.util.List;

import org.springframework.batch.item.support.AbstractItemStreamItemWriter;

import com.redislabs.recharge.Entity;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NilWriter extends AbstractItemStreamItemWriter<Entity> {

	private int currentItemCount = 0;

	public void write(List<? extends Entity> items) throws Exception {
		currentItemCount += items.size();
		log.info("NoOpped {} items", currentItemCount);
	}
}
