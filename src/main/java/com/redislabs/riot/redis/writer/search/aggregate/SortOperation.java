package com.redislabs.riot.redis.writer.search.aggregate;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redislabs.lettusearch.aggregate.Order;

import lombok.Data;

@Data
public class SortOperation {

	private Map<String, Order> properties = new LinkedHashMap<>();
	private Long max;

}