package com.redislabs.recharge.generator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.redislabs.recharge.batch.RediSearchWriter;
import com.redislabs.recharge.config.Recharge;

import io.redisearch.Schema;
import io.redisearch.client.Client;

@Component
public class RecordItemWriter extends ItemStreamSupport implements ItemWriter<Record> {

	@Autowired
	private RecordRepository repository;

	@Autowired
	private RediSearchWriter rediSearchWriter;

	@Autowired
	private Recharge config;

	@Override
	public void open(ExecutionContext executionContext) {
		rediSearchWriter.open(executionContext);
		Schema schema = new Schema();
		schema.addTextField("firstName", 1);
		schema.addTextField("lastName", 1);
		schema.addTextField("state", 1);
		schema.addSortableNumericField("year");
		try {
			rediSearchWriter.createIndex(schema, Client.IndexOptions.Default());
		} catch (Exception e) {
			// ignore
		}
	}

	@Override
	public void write(List<? extends Record> items) throws Exception {
		for (Record item : items) {
			Record record = repository.save(item);
			if (config.getRedisearch().isEnabled()) {
				Map<String, Object> fields = new HashMap<>();
				fields.put("firstName", record.getFirstName());
				fields.put("lastName", record.getLastName());
				fields.put("state", record.getState());
				fields.put("year", record.getYear());
				rediSearchWriter.addDocument(record.getId(), fields);
			}
		}
	}

}
