package com.redis.riot;

import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.LimitCheckingItemSkipPolicy;
import org.springframework.batch.core.step.skip.NeverSkipItemSkipPolicy;
import org.springframework.batch.core.step.skip.SkipPolicy;

public enum StepSkipPolicy {

	ALWAYS(new AlwaysSkipItemSkipPolicy()), NEVER(new NeverSkipItemSkipPolicy()),
	LIMIT(new LimitCheckingItemSkipPolicy());

	private final SkipPolicy skipPolicy;

	private StepSkipPolicy(SkipPolicy skipPolicy) {
		this.skipPolicy = skipPolicy;
	}

	public SkipPolicy getSkipPolicy() {
		return skipPolicy;
	}

}