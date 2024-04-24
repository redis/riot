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

import com.redis.riot.core.AbstractImport;
import com.redis.spring.batch.KeyValue;
import com.redis.spring.batch.RedisItemWriter;

public class FileDumpImport extends AbstractImport {

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
	protected Job job() {
		List<Resource> resources = FileUtils.inputResources(files, fileOptions);
		if (resources.isEmpty()) {
			throw new IllegalArgumentException("No file found");
		}
		List<TaskletStep> steps = new ArrayList<>();
		for (Resource resource : resources) {
			RedisItemWriter<String, String, KeyValue<String, Object>> writer = RedisItemWriter.struct();
			configure(writer);
			steps.add(step(resource.getFilename(), reader(resource), writer).build());
		}
		Iterator<TaskletStep> iterator = steps.iterator();
		SimpleJobBuilder job = jobBuilder().start(iterator.next());
		while (iterator.hasNext()) {
			job.next(iterator.next());
		}
		return job.build();
	}

	private ItemReader<KeyValue<String, Object>> reader(Resource resource) {
		if (type == FileDumpType.XML) {
			return FileUtils.xmlReader(resource, KeyValue.class);
		}
		return FileUtils.jsonReader(resource, KeyValue.class);
	}

}
