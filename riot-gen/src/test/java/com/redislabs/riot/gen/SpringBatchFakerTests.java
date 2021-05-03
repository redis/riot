package com.redislabs.riot.gen;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
@SpringBootTest(classes = BatchTestApplication.class)
@RunWith(SpringRunner.class)
public class SpringBatchFakerTests {

	@Autowired
	private JobLauncher jobLauncher;
	@Autowired
	private JobBuilderFactory jobBuilderFactory;
	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Test
	public void testReader() throws Exception {
		int count = 100;
		Map<String, String> fields = new HashMap<>();
		fields.put("firstName", "name.firstName");
		fields.put("lastName", "name.lastName");
		FakerItemReader reader = FakerItemReader.builder().generator(MapGenerator.builder().fields(fields).build()).end(count).build();
		List<Map<String, Object>> items = new ArrayList<>();
		run("reader", reader, items::addAll);
		Assertions.assertEquals(count, items.size());
		Assertions.assertFalse(items.get(0).containsKey(MapGenerator.FIELD_INDEX));
		Assertions.assertTrue(((String) items.get(0).get("firstName")).length() > 0);
		Assertions.assertTrue(((String) items.get(0).get("lastName")).length() > 0);
	}

	@Test
	public void testIncludeMetadata() throws Exception {
		int count = 100;
		Map<String, String> fields = new HashMap<>();
		fields.put("firstName", "name.firstName");
		fields.put("lastName", "name.lastName");
		FakerItemReader reader = FakerItemReader.builder().generator(new MapWithMetadataGenerator(MapGenerator.builder().fields(fields).build())).end(count).build();
		List<Map<String, Object>> items = new ArrayList<>();
		run("metadata", reader, items::addAll);
		Assertions.assertEquals(count, items.size());
		Assertions.assertEquals(1L, items.get(0).get(MapGenerator.FIELD_INDEX));
	}

	private <T> void run(String name, ItemReader<T> reader, ItemWriter<T> writer) throws Exception {
		TaskletStep step = stepBuilderFactory.get(name + "-step").<T, T>chunk(50).reader(reader).writer(writer).build();
		Job job = jobBuilderFactory.get(name + "-job").start(step).build();
		jobLauncher.run(job, new JobParameters());
	}

}