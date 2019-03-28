package com.redislabs.recharge.generator;

import java.util.Locale;
import java.util.Random;

import org.ruaux.pojofaker.Faker;

public class GeneratorRootObject extends Faker {

	private GeneratorReader reader;

	public GeneratorRootObject() {
		super();
	}

	public GeneratorRootObject(Locale locale, Random random) {
		super(locale, random);
	}

	public GeneratorRootObject(Locale locale) {
		super(locale);
	}

	public GeneratorRootObject(Random random) {
		super(random);
	}

	public void setReader(GeneratorReader reader) {
		this.reader = reader;
	}

	public long index(long end) {
		return reader.current(end);
	}

	public long segment(long end) {
		return reader.segment(end);
	}

	public long segment(long start, long end) {
		return reader.segment(start, end);
	}

	public int getIndex() {
		return reader.current();
	}

	public long nextLong(long end) {
		return reader.nextLong(end);
	}

	public long nextLong(long start, long end) {
		return reader.nextLong(start, end);
	}

	public String nextId(long start, long end, String format) {
		return reader.nextId(start, end, format);
	}

	public String nextId(long end, String format) {
		return reader.nextId(end, format);
	}

}
