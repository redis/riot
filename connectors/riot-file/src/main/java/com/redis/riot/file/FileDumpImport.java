package com.redis.riot.file;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.SimpleJobBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.core.io.Resource;

import com.redis.riot.core.AbstractStructImport;
import com.redis.riot.core.RiotContext;
import com.redis.riot.core.StepBuilder;
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
    protected Job job(RiotContext executionContext) {
        Iterator<Step> steps = FileUtils.inputResources(files, fileOptions).stream().map(r -> step(executionContext, r))
                .map(StepBuilder::build).iterator();
        if (!steps.hasNext()) {
            throw new IllegalArgumentException("No file found");
        }
        SimpleJobBuilder job = jobBuilder().start(steps.next());
        while (steps.hasNext()) {
            job.next(steps.next());
        }
        return job.build();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private StepBuilder<KeyValue<String>, KeyValue<String>> step(RiotContext executionContext, Resource resource) {
        StepBuilder<KeyValue<String>, KeyValue<String>> step = createStep();
        step.name(resource.getDescription());
        step.reader((ItemReader) reader(resource));
        step.writer(writer(executionContext));
        return step;
    }

    @SuppressWarnings("rawtypes")
    private ItemReader<KeyValue> reader(Resource resource) {
        if (type == FileDumpType.XML) {
            return FileUtils.xmlReader(resource, KeyValue.class);
        }
        return FileUtils.jsonReader(resource, KeyValue.class);
    }

}
