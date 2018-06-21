package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.batch.RediSearchWriter;
import com.redislabs.recharge.config.Recharge;

@Component
public class RecordItemWriter extends ItemStreamSupport implements ItemWriter<Record> {

	@Autowired
	private RecordRepository repository;

	@Autowired
	private RediSearchWriter rediSearchWriter;

	@Autowired
	private Recharge config;

	@Override
	public void write(List<? extends Record> items) throws Exception {
		for (Record record : items) {
			repository.save(record);
			if (config.getRedisearch().isEnabled()) {
				Map<String, Object> fields = new HashMap<>();
				fields.put("firstName", record.getFirstName());
				fields.put("lastName", record.getLastName());
				fields.put("state", record.getState());
				rediSearchWriter.addDocument(record.getId(), fields);
			}
		}
	}

}
