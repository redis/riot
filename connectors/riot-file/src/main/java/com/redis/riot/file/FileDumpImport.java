package com.redis.riot.file;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.Resource;

import com.redis.riot.core.AbstractStructImport;
import com.redis.riot.core.RiotContext;
import com.redis.spring.batch.RedisItemWriter;
import com.redis.spring.batch.common.KeyValue;

public class FileDumpImport extends AbstractStructImport {

	private List<String> files;

	private FileOptions fileOptions = new FileOptions();

	private FileDumpType type;

	public void setFiles(String... files) {
		setFiles(Arrays.asList(files));
	}

	public void setFiles(List<String> files) {
		this.files = files;
	}

	public void setFileOptions(FileOptions fileOptions) {
		this.fileOptions = fileOptions;
	}

	public void setType(FileDumpType type) {
		this.type = type;
	}

	@Override
	protected Job job(RiotContext executionContext) throws Exception {
		List<Resource> resources = FileUtils.inputResources(files, fileOptions);
		if (resources.isEmpty()) {
			throw new IllegalArgumentException("No file found");
		}
		List<TaskletStep> steps = new ArrayList<>();
		for (Resource resource : resources) {
			ItemReader<KeyValue<String>> reader = reader(resource);
			RedisItemWriter<String, String, KeyValue<String>> writer = writer(executionContext);
			steps.add(step(resource.getFilename(), reader, null, writer).build());
		}
		Iterator<TaskletStep> iterator = steps.iterator();
		SimpleJobBuilder job = jobBuilder().start(iterator.next());
		while (iterator.hasNext()) {
			job.next(iterator.next());
		}
		return job.build();
	}

	private ItemReader<KeyValue<String>> reader(Resource resource) {
		if (type == FileDumpType.XML) {
			return FileUtils.xmlReader(resource, KeyValue.class);
		}
		return FileUtils.jsonReader(resource, KeyValue.class);
	}

}
