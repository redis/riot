package com.redis.riot.cli;

import org.springframework.expression.Expression;

import com.redis.riot.core.ExportProcessorOptions;
import com.redis.riot.core.TemplateExpression;

import picocli.CommandLine.Option;

public class KeyValueProcessorArgs {

	@Option(names = "--key-proc", description = "SpEL template expression to transform the name of each key. E.g. --key-proc=\"#{#source.database}:#{key}\" transform key 'test:1' into '0:test:1'", paramLabel = "<exp>")
	private TemplateExpression keyExpression;

	@Option(names = "--type-proc", description = "SpEL expression to transform the type of each key.", paramLabel = "<exp>")
	private Expression typeExpression;

	@Option(names = "--ttl-proc", description = "SpEL expression to transform the TTL of each key.", paramLabel = "<exp>")
	private Expression ttlExpression;

	@Option(names = "--no-ttl", description = "Ignore key expiration TTLs from source instead of passing them along to the target.")
	private boolean dropTtl;

	@Option(names = "--no-stream-id", description = "Drop IDs from source stream messages instead of passing them along to the target.")
	private boolean dropStreamMessageIds;

	public ExportProcessorOptions processorOptions() {
		ExportProcessorOptions options = new ExportProcessorOptions();
		options.setKeyExpression(keyExpression);
		options.setTtlExpression(ttlExpression);
		options.setTypeExpression(typeExpression);
		options.setDropTtl(dropTtl);
		options.setDropStreamMessageId(dropStreamMessageIds);
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

	public boolean isDropTtl() {
		return dropTtl;
	}

	public void setDropTtl(boolean dropTtl) {
		this.dropTtl = dropTtl;
	}

	public boolean isDropStreamMessageIds() {
		return dropStreamMessageIds;
	}

	public void setDropStreamMessageIds(boolean dropStreamMessageIds) {
		this.dropStreamMessageIds = dropStreamMessageIds;
	}

}
