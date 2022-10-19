package com.redis.riot.file;

/**
 * Exception representing any errors encountered during I/O processing.
 * 
 */
@SuppressWarnings("serial")
public class RuntimeIOException extends RuntimeException {

	/**
	 * @param message the String that contains a detailed message.
	 */
	public RuntimeIOException(String message) {
		super(message);
	}

	/**
	 * Constructs a new instance with a message and nested exception.
	 * 
	 * @param msg    the exception message.
	 * @param nested the cause of the exception.
	 * 
	 */
	public RuntimeIOException(String msg, Throwable nested) {
		super(msg, nested);
	}

	/**
	 * Constructs a new instance with a nested exception and empty message.
	 *
	 * @param nested the cause of the exception.
	 */
	public RuntimeIOException(Throwable nested) {
		super(nested);
	}
}
