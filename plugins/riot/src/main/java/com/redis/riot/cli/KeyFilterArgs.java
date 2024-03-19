package com.redis.riot.cli;

import java.util.List;

import com.redis.riot.core.KeyFilterOptions;
import com.redis.spring.batch.common.Range;

import picocli.CommandLine.Option;

public class KeyFilterArgs {

	@Option(names = "--key-include", arity = "1..*", description = "Glob pattern to match keys for inclusion. E.g. 'mykey:*' will only consider keys starting with 'mykey:'.", paramLabel = "<exp>")
	List<String> includes;

	@Option(names = "--key-exclude", arity = "1..*", description = "Glob pattern to match keys for exclusion. E.g. 'mykey:*' will exclude keys starting with 'mykey:'.", paramLabel = "<exp>")
	List<String> excludes;

	@Option(names = "--key-slots", arity = "1..*", description = "Ranges of key slots to consider for processing. For example '0:8000' will only consider keys that fall within the range 0 to 8000.", paramLabel = "<range>")
	List<Range> slots;

	public KeyFilterOptions keyFilterOptions() {
		KeyFilterOptions options = new KeyFilterOptions();
		options.setExcludes(excludes);
		options.setIncludes(includes);
		options.setSlots(slots);
		return options;
	}

}
