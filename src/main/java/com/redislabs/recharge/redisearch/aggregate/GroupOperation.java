package com.redislabs.recharge.redisearch.aggregate;

import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class GroupOperation {

	private List<String> properties = new ArrayList<>();
	private List<ReduceFunction> reducers = new ArrayList<>();

}
