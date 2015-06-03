package com.qubole.kinesis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class RecordReader implements Iterator<Record> {
  private final static Logger LOGGER = Logger.getLogger(RecordReader.class
      .getName());

  private BufferedReader br;
  private String nextLine;
  private int lineNumber = 1;
  private int skippedRecords = 0;
  private int numRecords = 0;

  public RecordReader(InputStream is) {
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
  public boolean hasNext() {
    return (nextLine != null && nextLine.length() > 0);
  }

  @Override
  public Record next() {
    Record rec = null;
    do {
      rec = parseRecord(nextLine);
      if (rec == null) {
        skippedRecords++;
      }
      readAhead();
    } while (rec == null && hasNext());
    numRecords++;
    return rec;
  }

  private static Pattern pattern = Pattern
      .compile("(?<ip>\\w+(\\.\\w+)*)\\s\\-\\s\\-\\s\\[(?<ts>.*)\\]\\s\\\"(?<req>.*)\\\"\\s(?<code>\\d+)\\s(?<bytes>(\\d+|\\-))");

  // 01/Jul/1995:00:00:09 -0400
  private static DateTimeFormatter dtf = DateTimeFormat
      .forPattern("dd/MMM/YYYY:HH:mm:ss Z");

  private Record parseRecord(String line) {
    Matcher m = pattern.matcher(line);
    if (!m.find()) {
      LOGGER.log(Level.SEVERE, "skipping line number " + lineNumber
          + " (unable to parse):" + line);
      return null;
    }
    Record rec = new Record();
    try {
      rec.setHost(m.group("ip"));
      rec.setTimestamp(dtf.parseDateTime(m.group("ts")));
      rec.setRequest(m.group("req"));
      rec.setReplyCode(Integer.parseInt(m.group("code")));
      if (m.group("bytes").equals("-")) {
        rec.setReplyBytes(0);
      } else {
        rec.setReplyBytes(Integer.parseInt(m.group("bytes")));
      }
    } catch (RuntimeException e) {
      LOGGER.log(Level.SEVERE, "skipping line number " + lineNumber
          + " due to " + e.getMessage());
      return null;
    }
    return rec;
  }

  @Override
  public void remove() {
    throw new NotImplementedException();
  }

  public long getNumRecords() {
    return numRecords;
  }

  public long getSkippedRecords() {
    return skippedRecords;
  }
}
