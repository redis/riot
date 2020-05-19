package com.redislabs.riot.cli.file;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.DefaultBufferedReaderFactory;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.DelimitedBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder.FixedLengthBuilder;
import org.springframework.batch.item.file.separator.DefaultRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.Range;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.batch.item.xml.XmlItemReader;
import org.springframework.batch.item.xml.XmlObjectReader;
import org.springframework.batch.item.xml.builder.XmlItemReaderBuilder;
import org.springframework.core.io.Resource;
import org.springframework.util.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine.Option;

@Slf4j
public class FileImportOptions extends FileOptions {

    @Getter
    @Option(names = {"--skip"}, description = "Lines to skip from the beginning of the file (default: ${DEFAULT-VALUE})", paramLabel = "<count>")
    private int linesToSkip = 0;
    @Getter
    @Option(names = "--include", arity = "1..*", description = "Field indices to include (0-based)", paramLabel = "<index>")
    private List<Integer> includedFields = new ArrayList<>();
    @Getter
    @Option(names = "--ranges", arity = "1..*", description = "Fixed-width column ranges", paramLabel = "<int>")
    private List<Range> columnRanges = new ArrayList<>();
    @Getter
    @Option(names = {"-q",
            "--quote"}, description = "Escape character (default: ${DEFAULT-VALUE})", paramLabel = "<char>")
    private Character quoteCharacter = DelimitedLineTokenizer.DEFAULT_QUOTE_CHARACTER;

}
