package com.redis.riot;

import java.util.function.UnaryOperator;

import com.redis.riot.function.StreamOperator;
import com.redis.spring.batch.item.redis.common.KeyValue;

import picocli.CommandLine.Option;

public class StreamProcessorArgs {

	@Option(names = "--stream-ids", description = "Propagate stream message IDs. True by default.", negatable = true, defaultValue = "true", fallbackValue = "true")
	private boolean propagateIds = true;

	@Option(names = "--stream-prune", description = "Drop empty streams.")
	private boolean prune;

	public UnaryOperator<KeyValue<String, Object>> operator() {
		if (propagateIds && !prune) {
			return null;
		}
		StreamOperator operator = new StreamOperator();
		operator.setDropMessageIds(!propagateIds);
		operator.setPrune(prune);
		return operator;
	}

	public boolean isPropagateIds() {
		return propagateIds;
	}

	public void setPropagateIds(boolean propagateIds) {
		this.propagateIds = propagateIds;
	}

	public boolean isPrune() {
		return prune;
	}

	public void setPrune(boolean prune) {
		this.prune = prune;
	}

	@Override
	public String toString() {
		return "StreamProcessorArgs [propagateIds=" + propagateIds + ", prune=" + prune + "]";
	}

}
