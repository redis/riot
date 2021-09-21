package com.redis.riot.file;

import com.redis.riot.AbstractTransferCommand;
import com.redis.riot.RedisOptions;
import com.redis.riot.RiotStepBuilder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Data
@EqualsAndHashCode(callSuper = true)
@Command(name = "import-dump", description = "Import Redis data files into Redis")
public class DumpFileImportCommand extends AbstractTransferCommand {

    @CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private List<String> files;
    @CommandLine.Mixin
    private DumpFileImportOptions options = new DumpFileImportOptions();

    @Override
    protected Flow flow(StepBuilderFactory stepBuilderFactory) throws Exception {
        Assert.isTrue(!ObjectUtils.isEmpty(files), "No file specified");
        List<String> expandedFiles = FileUtils.expand(files);
        if (ObjectUtils.isEmpty(expandedFiles)) {
            throw new FileNotFoundException("File not found: " + String.join(", ", files));
        }
        List<Step> steps = new ArrayList<>();
        DataStructureItemProcessor processor = new DataStructureItemProcessor();
        for (String file : expandedFiles) {
            DumpFileType fileType = fileType(file);
            Resource resource = options.inputResource(file);
            AbstractItemStreamItemReader<DataStructure> reader = reader(fileType, resource);
            reader.setName(file + "-reader");
            StepBuilder stepBuilder = stepBuilderFactory.get(file + "-datastructure-file-import-step");
            RiotStepBuilder<DataStructure, DataStructure> step = riotStep(stepBuilder, "Importing " + file);
            steps.add(step.reader(reader).processor(processor).writer(writer()).build().build());
        }
        return flow(steps.toArray(new Step[0]));
    }

    private DumpFileType fileType(String file) {
        if (options.getType() == null) {
            return DumpFileType.of(file);
        }
        return options.getType();
    }

    private ItemWriter<DataStructure> writer() {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            return DataStructureItemWriter.client(redisOptions.clusterClient()).poolConfig(redisOptions.poolConfig()).build();
        }
        return DataStructureItemWriter.client(redisOptions.client()).poolConfig(redisOptions.poolConfig()).build();
    }

    protected AbstractItemStreamItemReader<DataStructure> reader(DumpFileType fileType, Resource resource) {
        if (fileType == DumpFileType.XML) {
            log.debug("Creating XML data structure reader for file {}", resource);
            return FileUtils.xmlReader(resource, DataStructure.class);
        }
        log.debug("Creating JSON data structure reader for file {}", resource);
        return FileUtils.jsonReader(resource, DataStructure.class);
    }

}
