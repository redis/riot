package com.redis.riot.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.support.StandardEvaluationContext;

class ProcessorTests {

	@Test
	void testMapProcessor() throws Exception {
		Map<String, Expression> expressions = new LinkedHashMap<>();
		expressions.put("field1", RiotUtils.parse("'test:1'"));
		MapProcessorArgs processorArgs = new MapProcessorArgs();
		processorArgs.setExpressions(expressions);
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = processorArgs
				.processor(evaluationContext());
		Map<String, Object> map = processor.process(new HashMap<>());
		Assertions.assertEquals("test:1", map.get("field1"));
		// Assertions.assertEquals("1", map.get("id"));
	}

	private StandardEvaluationContext evaluationContext() {
		return new StandardEvaluationContext();
	}

	@Test
	void processor() throws Exception {
		Map<String, Expression> expressions = new LinkedHashMap<>();
		expressions.put("field1", RiotUtils.parse("'value1'"));
		expressions.put("field2", RiotUtils.parse("field1"));
		expressions.put("field3", RiotUtils.parse("1"));
		expressions.put("field4", RiotUtils.parse("2"));
		expressions.put("field5", RiotUtils.parse("field3+field4"));
		MapProcessorArgs options = new MapProcessorArgs();
		options.setExpressions(expressions);
		StandardEvaluationContext evaluationContext = evaluationContext();
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor(evaluationContext);
		for (int index = 0; index < 10; index++) {
			Map<String, Object> result = processor.process(new HashMap<>());
			assertEquals(5, result.size());
			assertEquals("value1", result.get("field1"));
			assertEquals("value1", result.get("field2"));
			assertEquals(3, result.get("field5"));
		}
	}

	@Test
	void processorFilter() throws Exception {
		MapProcessorArgs options = new MapProcessorArgs();
		options.setFilter(RiotUtils.parse("index<10"));
		StandardEvaluationContext evaluationContext = evaluationContext();
		evaluationContext.addPropertyAccessor(new QuietMapAccessor());
		ItemProcessor<Map<String, Object>, Map<String, Object>> processor = options.processor(evaluationContext);
		for (int index = 0; index < 100; index++) {
			Map<String, Object> map = new HashMap<>();
			map.put("index", index);
			Map<String, Object> result = processor.process(map);
			if (index < 10) {
				Assertions.assertNotNull(result);
			} else {
				Assertions.assertNull(result);
			}
		}
	}

}
