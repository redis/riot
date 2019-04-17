package com.redislabs.riot.processor;

import java.util.Map;

import org.springframework.batch.item.ItemProcessor;

import lombok.Setter;

@SuppressWarnings("rawtypes")
public class SpelProcessor implements ItemProcessor<Map, Map> {

	@Setter
	private ItemProcessor<Map, Map> sourceProcessor;
	@Setter
	private ItemProcessor<Map, Map> mergeProcessor;
	@Setter
	private ItemProcessor<Map, Map> fieldProcessor;

	@Override
	public Map process(Map record) throws Exception {
		Map map = sourceProcessor.process(record);
		mergeProcessor.process(map);
		fieldProcessor.process(map);
		return map;
	}

}
