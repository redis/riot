package com.redislabs.recharge.generator;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;

import com.github.javafaker.Address;
import com.github.javafaker.Faker;
import com.github.javafaker.Name;

@Component
public class RecordItemReader extends AbstractItemCountingItemStreamItemReader<Record> {

	@Autowired
	private FakerProvider provider;

	public RecordItemReader() {
		setName(ClassUtils.getShortName(RecordItemReader.class));
	}

	@SuppressWarnings("deprecation")
	@Override
	protected Record doRead() throws Exception {
		Faker faker = provider.getFaker();
		Name name = faker.name();
		Address address = faker.address();
		Record record = new Record();
		record.setFirstName(name.firstName());
		record.setLastName(name.lastName());
		record.setState(address.stateAbbr());
		record.setYear(faker.date().birthday().getYear());
		return record;
	}

	@Override
	protected void doOpen() throws Exception {
		// do nothing
	}

	@Override
	protected void doClose() throws Exception {
		// do nothing
	}

}
