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
	private TtlArgs expireArgs = new TtlArgs();

	@Option(names = "--abs", description = "Timeout is POSIX time in milliseconds.")
	private boolean absolute;

	public static class TtlArgs {

		@Option(names = "--ttl-field", required = true, description = "Expire timeout field.", paramLabel = "<field>")
		private String ttlField;

		@Option(names = "--ttl", required = true, description = "Expire timeout duration in milliseconds.", paramLabel = "<ms>")
		private long ttlValue;

		public String getTtlField() {
			return ttlField;
		}

		public void setTtlField(String ttlField) {
			this.ttlField = ttlField;
		}

		public long getTtlValue() {
			return ttlValue;
		}

		public void setTtlValue(long ttlValue) {
			this.ttlValue = ttlValue;
		}

	}

	@Override
	public AbstractWriteOperation<String, String, Map<String, Object>> operation() {
		if (absolute) {
			ExpireAt<String, String, Map<String, Object>> operation = new ExpireAt<>(keyFunction());
			if (isTtlValue()) {
				operation.setTimestamp(expireArgs.getTtlValue());
			} else {
				operation.setTimestampFunction(fieldTtl());
			}
			return operation;
		}
		Expire<String, String, Map<String, Object>> operation = new Expire<>(keyFunction());
		if (isTtlValue()) {
			operation.setTtl(expireArgs.getTtlValue());
		} else {
			operation.setTtlFunction(fieldTtl());
		}
		return operation;
	}

	private ToLongFunction<Map<String, Object>> fieldTtl() {
		return toLong(expireArgs.getTtlField());
	}

	private boolean isTtlValue() {
		return expireArgs.getTtlValue() > 0;
	}

	public TtlArgs getExpireArgs() {
		return expireArgs;
	}

	public void setExpireArgs(TtlArgs expireArgs) {
		this.expireArgs = expireArgs;
	}

	public boolean isAbsolute() {
		return absolute;
	}

	public void setAbsolute(boolean absolute) {
		this.absolute = absolute;
	}

}