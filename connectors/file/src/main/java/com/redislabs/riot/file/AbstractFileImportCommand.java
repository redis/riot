package com.redislabs.riot.file;

import com.redislabs.riot.AbstractImportCommand;
import lombok.Getter;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

@Command
public abstract class AbstractFileImportCommand<T> extends AbstractImportCommand<T, T> {

    @SuppressWarnings("unused")
    @CommandLine.Parameters(arity = "1..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private String[] files;
    @Getter
    @CommandLine.Mixin
    private FileOptions fileOptions = FileOptions.builder().build();

    @Override
    protected Flow flow() throws Exception {
        List<String> expandedFiles = FileUtils.expand(files);
        if (ObjectUtils.isEmpty(expandedFiles)) {
            throw new FileNotFoundException("File not found: " + String.join(", ", files));
        }
        List<Step> steps = new ArrayList<>();
        for (String file : expandedFiles) {
            FileType fileType = fileOptions.type(file);
            Resource resource = FileUtils.inputResource(file, fileOptions);
            AbstractItemStreamItemReader<T> reader = reader(file, fileType, resource);
            String name = FileUtils.filename(resource);
            reader.setName(name);
            steps.add(step(name + "-file-import-step", "Importing " + name, reader).build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    protected abstract AbstractItemStreamItemReader<T> reader(String file, FileType fileType, Resource resource);

}
