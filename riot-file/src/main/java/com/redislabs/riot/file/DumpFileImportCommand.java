package com.redislabs.riot.file;

import com.redislabs.mesclun.RedisModulesClient;
import com.redislabs.riot.AbstractTransferCommand;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.RiotStepBuilder;
import io.lettuce.core.cluster.RedisClusterClient;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.redis.DataStructureItemWriter;
import org.springframework.batch.item.redis.support.DataStructure;
import org.springframework.batch.item.support.AbstractItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
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
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Command(name = "import-dump", description = "Import Redis data files into Redis")
public class DumpFileImportCommand extends AbstractTransferCommand {

    @CommandLine.Parameters(arity = "0..*", description = "One ore more files or URLs", paramLabel = "FILE")
    private List<String> files;
    @Builder.Default
    @CommandLine.Mixin
    private DumpFileImportOptions options = DumpFileImportOptions.builder().build();

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
            String name = FileUtils.filename(resource);
            AbstractItemStreamItemReader<DataStructure<String>> reader = reader(fileType, resource);
            reader.setName(name);
            StepBuilder stepBuilder = stepBuilderFactory.get(name + "-datastructure-file-import-step");
            RiotStepBuilder<DataStructure<String>, DataStructure<String>> step = riotStep(stepBuilder, "Importing " + name);
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

    private ItemWriter<DataStructure<String>> writer() {
        RedisOptions redisOptions = getRedisOptions();
        if (redisOptions.isCluster()) {
            RedisClusterClient client = redisOptions.redisClusterClient();
            return DataStructureItemWriter.client(client).poolConfig(redisOptions.poolConfig()).build();
        }
        RedisModulesClient client = redisOptions.redisClient();
        return DataStructureItemWriter.client(client).poolConfig(redisOptions.poolConfig()).build();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected AbstractItemStreamItemReader<DataStructure<String>> reader(DumpFileType fileType, Resource resource) {
        if (fileType == DumpFileType.XML) {
            log.debug("Creating XML data structure reader for file {}", resource);
            return (XmlItemReader) FileUtils.xmlReader(resource, DataStructure.class);
        }
        log.debug("Creating JSON data structure reader for file {}", resource);
        return (JsonItemReader) FileUtils.jsonReader(resource, DataStructure.class);
    }

}
