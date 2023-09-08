package com.redis.riot.file;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.function.FunctionItemProcessor;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.core.io.Resource;

import com.redis.riot.core.AbstractKeyValueImport;
import com.redis.riot.core.StepBuilder;
import com.redis.riot.file.resource.XmlItemReader;
import com.redis.spring.batch.KeyValue;

import io.lettuce.core.AbstractRedisClient;

public class FileDumpImport extends AbstractKeyValueImport {

    private final List<String> files;

    private FileOptions fileOptions = new FileOptions();

    private FileDumpType type;

    public FileDumpImport(AbstractRedisClient client, String... files) {
        this(client, Arrays.asList(files));
    }

    public FileDumpImport(AbstractRedisClient client, List<String> files) {
        super(client);
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
        Iterator<Step> steps = FileUtils.inputResources(files, fileOptions).stream().map(this::step).map(StepBuilder::build)
                .iterator();
        if (!steps.hasNext()) {
            throw new IllegalArgumentException("No file found");
        }
        SimpleJobBuilder job = jobBuilder().start(steps.next());
        while (steps.hasNext()) {
            job.next(steps.next());
        }
        return job.build();
    }

    private StepBuilder<KeyValue<String>, KeyValue<String>> step(Resource resource) {
        StepBuilder<KeyValue<String>, KeyValue<String>> step = createStep();
        step.name(resource.getDescription());
        step.reader(reader(resource));
        step.writer(writer());
        step.processor(processor());
        return step;
    }

    private ItemProcessor<KeyValue<String>, KeyValue<String>> processor() {
        return new FunctionItemProcessor<>(new FileDumpFunction());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ItemStreamReader<KeyValue<String>> reader(Resource resource) {
        if (type == FileDumpType.XML) {
            return (XmlItemReader) FileUtils.xmlReader(resource, KeyValue.class);
        }
        return (JsonItemReader) FileUtils.jsonReader(resource, KeyValue.class);
    }

}
