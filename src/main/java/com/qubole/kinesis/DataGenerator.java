package com.qubole.kinesis;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataGenerator {
  private final static Logger LOGGER = Logger.getLogger(DataGenerator.class
      .getName());

  private String stream = "";
  private boolean create = false;
  private int shards = 10;
  private int rate = 1000;
  private boolean verbose = false;
  private int workers = 1;
  private int records = 1000;
  private String sample = "";

  private static Options getOptions() {
    Options options = new Options();

    Option help = new Option("help", "print this message");
    options.addOption(help);

    Option verbose = new Option("verbose", "be extra verbose");
    options.addOption(verbose);

    Option create = new Option("create", "drop and recreate stream");
    options.addOption(create);

    Option stream = Option.builder("n").longOpt("kinesis-stream")
        .argName("name").desc("kinesis stream to write to (required)").hasArg().required().build();
    options.addOption(stream);

    Option shards = Option.builder("s").longOpt("num-shards").argName("number")
        .desc("number of shards").hasArg().build();
    options.addOption(shards);

    Option rate = Option.builder("r").longOpt("rate-limit").argName("number")
        .desc("number of records pushed per second (default 1000)").hasArg().build();
    options.addOption(rate);

    Option workers = Option.builder("w").longOpt("num-workers")
        .argName("number").desc("number of workers (default 1)").hasArg().build();
    options.addOption(workers);

    Option records = Option.builder("r").longOpt("num-records")
        .argName("number").desc("total number of records to push (default 1000)").hasArg().build();
    options.addOption(records);

    Option sample = Option.builder("f").longOpt("sample-file").argName("file")
        .desc("sample file to generate records (required)").hasArg().required().build();
    options.addOption(sample);
    
    return options;
  }

  private static void usage(int code) {
    usage(null, code);
  }

  private static void usage(String header, int code) {
    HelpFormatter formatter = new HelpFormatter();
    if (header != null) {
      formatter.printHelp("kinesis-datagen", header, getOptions(), "");
    } else {
      formatter.printHelp("kinesis-datagen", getOptions());
    }
    System.exit(code);
  }

  public static void main(String args[]) {
    CommandLineParser parser = new DefaultParser();
    Options options = getOptions();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      usage(e.getMessage(), 1);
    }
    if (cmd.hasOption("help")) {
      usage(0);
    }

    if (cmd.hasOption("num-shards") && !cmd.hasOption("create")) {
      usage("shards option can be specified only if create option is enabled",
          1);
    }
    String sample = cmd.getOptionValue("f");
    LOGGER.log(Level.INFO, "opening file " + sample);
    FileInputStream f = null;
    try {
      f = new FileInputStream(sample);
    } catch (FileNotFoundException e) {
      usage("sample file " + sample + " not found", 1);
    }
    RecordReader rr = new RecordReader(f);
    while(rr.hasNext()) {
      Record rec = rr.next();
      LOGGER.log(Level.INFO, "record " + rec);
    }
  }
}
