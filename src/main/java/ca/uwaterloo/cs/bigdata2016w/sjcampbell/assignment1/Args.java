package ca.uwaterloo.cs.bigdata2016w.sjcampbell.assignment1;

import org.kohsuke.args4j.Option;

public class Args {
	@Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
	public String input;

	@Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
	public String output;

	@Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
	public int numReducers = 1;
}