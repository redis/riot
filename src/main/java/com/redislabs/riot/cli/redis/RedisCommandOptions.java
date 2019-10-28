package com.redislabs.riot.cli.redis;

import java.util.ArrayList;
import java.util.List;

import com.redislabs.riot.batch.redis.writer.CollectionMapWriter;
import com.redislabs.riot.batch.redis.writer.DebugMapWriter;
import com.redislabs.riot.batch.redis.writer.HmsetMapWriter;
import com.redislabs.riot.batch.redis.writer.LpushMapWriter;
import com.redislabs.riot.batch.redis.writer.NoopMapWriter;
import com.redislabs.riot.batch.redis.writer.RedisMapWriter;
import com.redislabs.riot.batch.redis.writer.RpushMapWriter;
import com.redislabs.riot.batch.redis.writer.SaddMapWriter;
import com.redislabs.riot.cli.RedisCommand;

import lombok.Data;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public @Data class RedisCommandOptions {

	@Option(names = { "-c",
			"--command" }, description = "Command: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})", paramLabel = "<name>")
	private RedisCommand command = RedisCommand.hmset;
	@Option(names = "--members", arity = "1..*", description = "Member fields for collections: list geo set zset", paramLabel = "<names>")
	private List<String> members = new ArrayList<>();
	@ArgGroup(exclusive = false, heading = "Lua command options%n", order = 21)
	private LuaCommandOptions lua = new LuaCommandOptions();
	@ArgGroup(exclusive = false, heading = "Stream command options%n", order = 21)
	private StreamCommandOptions stream = new StreamCommandOptions();
	@ArgGroup(exclusive = false, heading = "String command options%n", order = 21)
	private StringCommandOptions string = new StringCommandOptions();
	@ArgGroup(exclusive = false, heading = "Sorted set command options%n", order = 21)
	private ZsetCommandOptions zset = new ZsetCommandOptions();
	@ArgGroup(exclusive = false, heading = "Geo command options%n", order = 21)
	private GeoCommandOptions geo = new GeoCommandOptions();
	@ArgGroup(exclusive = false, heading = "Expire command options%n", order = 21)
	private ExpireCommandOptions expire = new ExpireCommandOptions();

	public RedisMapWriter writer() {
		RedisMapWriter redisItemWriter = redisItemWriter(command);
		if (redisItemWriter instanceof CollectionMapWriter) {
			((CollectionMapWriter) redisItemWriter).setFields(members.toArray(new String[members.size()]));
		}
		return redisItemWriter;
	}

	private RedisMapWriter redisItemWriter(RedisCommand command) {
		switch (command) {
		case evalsha:
			return lua.writer();
		case expire:
			return expire.writer();
		case geoadd:
			return geo.writer();
		case lpush:
			return new LpushMapWriter();
		case rpush:
			return new RpushMapWriter();
		case sadd:
			return new SaddMapWriter();
		case set:
			return string.writer();
		case xadd:
			return stream.writer();
		case zadd:
			return zset.writer();
		case print:
			return new DebugMapWriter();
		case noop:
			return new NoopMapWriter();
		default:
			return new HmsetMapWriter();
		}
	}

}