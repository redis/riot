package com.redislabs.riot.cli.redis.command;

import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Language;
import com.redislabs.riot.redis.writer.map.FtAdd;
import com.redislabs.riot.redis.writer.map.FtAddPayload;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "ftadd", description = "Adds documents to an index")
public class FtAddCommand extends AbstractKeyRedisCommand {

	@Option(names = { "-i", "--index" }, description = "Name of the RediSearch index", paramLabel = "<name>")
	private String index;
	@Option(names = "--nosave", description = "Do not save docs, only index")
	private boolean noSave;
	@Option(names = "--replace", description = "UPSERT-style insertion")
	private boolean replace;
	@Option(names = "--partial", description = "Partial update (only applicable with replace)")
	private boolean partial;
	@Option(names = "--language", description = "Stemmer to use for indexing: ${COMPLETION-CANDIDATES}", paramLabel = "<string>")
	private Language language;
	@Option(names = "--if-condition", description = "Boolean expression for conditional update", paramLabel = "<exp>")
	private String ifCondition;
	@Option(names = "--payload", description = "Name of the field containing the payload", paramLabel = "<field>")
	private String payload;
	@Option(names = "--score", description = "Name of the field to use for scores", paramLabel = "<field>")
	private String score;
	@Option(names = "--default-score", description = "Score when field not present (default: ${DEFAULT-VALUE})", paramLabel = "<num>")
	private double defaultScore = 1d;

	@Override
	protected FtAdd keyWriter() {
		FtAdd writer = ftAdd();
		writer.options(AddOptions.builder().ifCondition(ifCondition).language(language).noSave(noSave).replace(replace)
				.replacePartial(partial).build());
		writer.index(index);
		writer.defaultScore(defaultScore);
		writer.scoreField(score);
		return writer;
	}

	private FtAdd ftAdd() {
		if (payload == null) {
			return new FtAdd();
		}
		FtAddPayload writer = new FtAddPayload();
		writer.payload(payload);
		return writer;
	}

}
