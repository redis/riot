package com.redis.riot;

import picocli.AutoComplete;
import picocli.CommandLine.Command;

@Command(hidden = true, name = "generate-completion")
public class GenerateCompletionCommand extends AutoComplete.GenerateCompletion {
}