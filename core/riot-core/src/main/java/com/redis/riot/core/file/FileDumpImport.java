package com.redis.riot.core.file;

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
import com.redis.riot.core.file.resource.XmlItemReader;
import com.redis.spring.batch.KeyValue;

import io.lettuce.core.AbstractRedisClient;

public class FileDumpImport extends AbstractKeyValueImport {

    private final List<String> files;

    private FileOptions fileOptions = new FileOptions();

    private FileDumpType type;

    public FileDumpImport(AbstractRedisClient client, List<String> files) {
        super(client);
        this.files = files;
    }

    public FileDumpType getType() {
        return type;
    }

    public void setType(FileDumpType type) {
        this.type = type;
    }

    public List<String> getFiles() {
        return files;
    }

    public FileOptions getFileOptions() {
        return fileOptions;
    }

    public void setFileOptions(FileOptions options) {
        this.fileOptions = options;
    }

    @Override
    protected Job job() {
        Iterator<Step> steps = FileUtils.inputResources(files, fileOptions).stream().map(this::step).iterator();
        if (!steps.hasNext()) {
            throw new IllegalArgumentException("No file found");
        }
        SimpleJobBuilder job = jobBuilder().start(steps.next());
        while (steps.hasNext()) {
            job.next(steps.next());
        }
        return job.build();
    }

    public Step step(Resource resource) {
        return step(resource.getDescription()).reader(reader(resource)).writer(writer()).processor(processor()).build().build();
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
