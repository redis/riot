package com.redislabs.recharge.redisearch.aggregate;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.recharge.redisearch.RediSearchCommandConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class AggregateConfiguration extends RediSearchCommandConfiguration {

	private String query;
	private boolean withSchema;
	private boolean verbatim;
	private List<String> loads = new ArrayList<>();
	private List<AggregateOperation> operations = new ArrayList<>();

}
