package com.redislabs.recharge.generator;

import java.util.List;

import org.springframework.batch.item.ItemStreamSupport;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RecordItemWriter extends ItemStreamSupport implements ItemWriter<Record> {

	@Autowired
	private RecordRepository repository;

	@Override
	public void write(List<? extends Record> items) throws Exception {
		repository.saveAll(items);
	}

}
