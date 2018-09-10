package com.redislabs.recharge.redis;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import com.redislabs.lettusearch.RediSearchClient;

import io.lettuce.core.RedisURI;

@Configuration
@EnableAutoConfiguration
public class RedisConfiguration {

	@Bean(destroyMethod = "shutdown")
	public RediSearchClient lettusearchClient(LettuceConnectionFactory connectionFactory) {
		LettuceClientConfiguration clientConfiguration = connectionFactory.getClientConfiguration();
		RedisURI uri = createRedisURIAndApplySettings(connectionFactory.getHostName(), connectionFactory.getPort(),
				connectionFactory.getPassword(), clientConfiguration);
		RediSearchClient rediSearchClient = clientConfiguration.getClientResources() //
				.map(clientResources -> RediSearchClient.create(clientResources, uri)) //
				.orElseGet(() -> RediSearchClient.create(uri));
		clientConfiguration.getClientOptions().ifPresent(rediSearchClient::setOptions);
		return rediSearchClient;
	}

	private RedisURI createRedisURIAndApplySettings(String hostName, int port, String password,
			LettuceClientConfiguration clientConfiguration) {
		RedisURI.Builder builder = RedisURI.Builder.redis(hostName, port);
		if (password != null) {
			builder.withPassword(password);
		}
		builder.withSsl(clientConfiguration.isUseSsl());
		builder.withVerifyPeer(clientConfiguration.isVerifyPeer());
		builder.withStartTls(clientConfiguration.isStartTls());
		builder.withTimeout(clientConfiguration.getCommandTimeout());
		return builder.build();
	}

}