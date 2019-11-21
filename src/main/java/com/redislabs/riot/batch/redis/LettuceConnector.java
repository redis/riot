package com.redislabs.riot.batch.redis;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public @Getter class LettuceConnector<K, V, C extends StatefulConnection<K, V>, R> {

	private AbstractRedisClient client;
	private Supplier<ClientResources> resources;
	private GenericObjectPool<C> pool;
	private Function<C, R> api;

}
