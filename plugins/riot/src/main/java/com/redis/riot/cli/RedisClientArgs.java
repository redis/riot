package com.redis.riot.cli;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.protocol.ProtocolVersion;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public class RedisClientArgs {

	@Option(names = "--auto-reconnect", description = "Auto-reconnect on connection loss (default: ${DEFAULT-VALUE}).", negatable = true)
	private boolean autoReconnect = ClientOptions.DEFAULT_AUTO_RECONNECT;

	@Option(names = "--resp", description = "Redis protocol version used to connect to Redis: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).", paramLabel = "<ver>")
	private ProtocolVersion protocolVersion = ProtocolVersion.RESP2;

	@ArgGroup(exclusive = false)
	private RedisSslArgs sslArgs = new RedisSslArgs();

}
