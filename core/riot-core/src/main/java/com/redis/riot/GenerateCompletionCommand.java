package com.redis.riot;

import picocli.AutoComplete;
import picocli.CommandLine.Command;

@Command(hidden = true, name = "generate-completion", usageHelpAutoWidth = true)
public class GenerateCompletionCommand extends AutoComplete.GenerateCompletion {
}