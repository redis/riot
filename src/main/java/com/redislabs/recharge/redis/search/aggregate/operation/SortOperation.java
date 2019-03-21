package com.redislabs.recharge.redis.search.aggregate.operation;

import java.util.LinkedHashMap;
import java.util.Map;

import com.redislabs.lettusearch.aggregate.SortProperty.Order;

import lombok.Data;

@Data
public class SortOperation {

	private Map<String, Order> properties = new LinkedHashMap<>();
	private Long max;

}