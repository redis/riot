package com.redislabs.recharge;

import java.util.Map;
import java.util.Map.Entry;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.recharge.RechargeConfiguration.EntityConfiguration;

public abstract class AbstractEntityItemReader extends AbstractItemCountingItemStreamItemReader<Entity> {

	private Entry<String, EntityConfiguration> entity;

	public AbstractEntityItemReader(Entry<String, EntityConfiguration> entity) {
		setName(entity.getKey() + "-" + ClassUtils.getShortName(this.getClass()));
		this.entity = entity;
	}

	@Override
	protected Entity doRead() throws Exception {
		return new Entity(entity.getKey(), readValues());
	}

	protected abstract Map<String, Object> readValues() throws Exception;

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
