package com.qubole.kinesis.text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.qubole.kinesis.core.StreamProducer;

public class FileLineReader implements StreamProducer<String> {
  private final static Logger LOGGER = Logger.getLogger(FileLineReader.class
      .getName());

  private BufferedReader br;
  private String nextLine;
  private long lineNumber = 1;
  private long numRecords = 0;

  public FileLineReader(InputStream is) {
    br = new BufferedReader(new InputStreamReader(is));
    readAhead();
  }

  private void readAhead() {
    try {
      nextLine = br.readLine();
      lineNumber++;
    } catch (IOException e) {
      LOGGER.log(Level.SEVERE, "unable to read line num " + lineNumber, e);
      nextLine = null;
    }
  }

  @Override
  public String next() {
    String line = nextLine;
    readAhead();
    if (line != null) {
      numRecords++;
    }
    return line;
  }

  @Override
  public void start() {
  }

  @Override
  public void end() {
    LOGGER.log(Level.INFO, "records = " + numRecords);
  }

  @Override
  public long records() {
    return numRecords;
  }
}
