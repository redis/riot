package com.redis.riot.core;

import java.io.PrintWriter;

public interface IO {

	PrintWriter getOut();

	void setOut(PrintWriter out);

	PrintWriter getErr();

	void setErr(PrintWriter err);

}
