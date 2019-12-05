package com.redislabs.riot.batch.redis;

import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.pool2.impl.GenericObjectPool;

import io.lettuce.core.AbstractRedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.resource.ClientResources;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

@AllArgsConstructor
@Accessors(fluent = true)
public @Getter class LettuceConnector<C extends StatefulConnection<String, String>, R> {

	private AbstractRedisClient client;
	private Supplier<C> connection;
	private Supplier<ClientResources> resources;
	private GenericObjectPool<C> pool;
	private Function<C, R> api;

}
