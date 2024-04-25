package com.redis.riot.cli;

import org.springframework.expression.Expression;

import com.redis.riot.core.KeyValueProcessorOptions;
import com.redis.riot.core.TemplateExpression;

import picocli.CommandLine.Option;

public class KeyValueProcessorArgs {

	@Option(names = "--key-proc", description = "SpEL template expression to transform the name of each key. E.g. \"#{#source.database}:#{key}\" with 'abc' returns '0:abc'", paramLabel = "<exp>")
	private TemplateExpression keyExpression;

	@Option(names = "--type-proc", description = "SpEL expression to transform the type of each key.", paramLabel = "<exp>")
	private Expression typeExpression;

	@Option(names = "--ttl-proc", description = "SpEL expression to transform the TTL of each key.", paramLabel = "<exp>")
	private Expression ttlExpression;

	@Option(names = "--ttls", negatable = true, defaultValue = "true", fallbackValue = "true", description = "Propagate key expiration TTLs from source to target. True by default.")
	private boolean propagateTtls = true;

	@Option(names = "--stream-ids", negatable = true, defaultValue = "true", fallbackValue = "true", description = "Propagate stream message IDs from source to target. True by default.")
	private boolean propagateStreamMessageIds = true;

	public KeyValueProcessorOptions processorOptions() {
		KeyValueProcessorOptions options = new KeyValueProcessorOptions();
		options.setKeyExpression(keyExpression);
		options.setTtlExpression(ttlExpression);
		options.setTypeExpression(typeExpression);
		options.setDropTtl(!propagateTtls);
		options.setDropStreamMessageId(!propagateStreamMessageIds);
		return options;
	}

	public TemplateExpression getKeyExpression() {
		return keyExpression;
	}

	public void setKeyExpression(TemplateExpression keyExpression) {
		this.keyExpression = keyExpression;
	}

	public Expression getTypeExpression() {
		return typeExpression;
	}

	public void setTypeExpression(Expression typeExpression) {
		this.typeExpression = typeExpression;
	}

	public Expression getTtlExpression() {
		return ttlExpression;
	}

	public void setTtlExpression(Expression ttlExpression) {
		this.ttlExpression = ttlExpression;
	}

	public boolean isPropagateTtls() {
		return propagateTtls;
	}

	public void setPropagateTtls(boolean propagateTtls) {
		this.propagateTtls = propagateTtls;
	}

	public boolean isPropagateStreamMessageIds() {
		return propagateStreamMessageIds;
	}

	public void setPropagateStreamMessageIds(boolean propagateStreamMessageIds) {
		this.propagateStreamMessageIds = propagateStreamMessageIds;
	}

}
