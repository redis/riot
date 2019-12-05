package com.redislabs.riot.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.experimental.Accessors;

@AllArgsConstructor
@Accessors(fluent = true)
public @Data class TransferContext {

	private int thread;
	private int threads;

}
