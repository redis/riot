package com.redislabs.riot;

import java.util.Map;

import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.util.ClassUtils;

import com.redislabs.riot.generator.GeneratorReader;

public abstract class AbstractReader extends AbstractItemCountingItemStreamItemReader<Map<String, Object>> {

	public AbstractReader() {
		setName(ClassUtils.getShortName(GeneratorReader.class));
	}

}
