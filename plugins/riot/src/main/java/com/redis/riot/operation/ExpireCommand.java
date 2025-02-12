package com.redis.riot.operation;

import java.util.Map;
import java.util.function.ToLongFunction;

import com.redis.spring.batch.item.redis.writer.impl.AbstractWriteOperation;
import com.redis.spring.batch.item.redis.writer.impl.Expire;
import com.redis.spring.batch.item.redis.writer.impl.ExpireAt;

import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "expire", description = "Set timeouts on keys")
public class ExpireCommand extends AbstractOperationCommand {

	public static final long DEFAULT_TTL = 60;

	@ArgGroup(exclusive = true)
	private ExpireTtlArgs ttlArgs = new ExpireTtlArgs();

	@Option(names = "--abs", description = "TTL is a POSIX time in milliseconds.")
	private boolean absolute;

	@Override
	public AbstractWriteOperation<String, String, Map<String, Object>> operation() {
		if (absolute) {
			ExpireAt<String, String, Map<String, Object>> operation = new ExpireAt<>(keyFunction());
			if (ttlArgs.getTtlValue() > 0) {
				operation.setTimestamp(ttlArgs.getTtlValue());
			} else {
				operation.setTimestampFunction(fieldTtl());
			}
			return operation;
		}
		Expire<String, String, Map<String, Object>> operation = new Expire<>(keyFunction());
		if (ttlArgs.getTtlValue() > 0) {
			operation.setTtl(ttlArgs.getTtlValue());
		} else {
			operation.setTtlFunction(fieldTtl());
		}
		return operation;
	}

	private ToLongFunction<Map<String, Object>> fieldTtl() {
		return toLong(ttlArgs.getTtlField());
	}

	public ExpireTtlArgs getTtlArgs() {
		return ttlArgs;
	}

	public void setTtlArgs(ExpireTtlArgs expireArgs) {
		this.ttlArgs = expireArgs;
	}

	public boolean isAbsolute() {
		return absolute;
	}

	public void setAbsolute(boolean absolute) {
		this.absolute = absolute;
	}

}