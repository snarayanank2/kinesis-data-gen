package com.qubole.kinesis.nasa;

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.qubole.kinesis.core.StreamConsumer;

public class RecordParser implements StreamConsumer<String> {
  private final static Logger LOGGER = Logger.getLogger(RecordParser.class
      .getName());

  private long skippedRecords = 0;
  private long numRecords = 0;
  protected Record lastRecord = null;
  protected String lastRecordJson = null;
  private DateTime startTime;
  
  private ObjectMapper om = new ObjectMapper();

  public RecordParser() {
    om.enable(SerializationFeature.INDENT_OUTPUT);
    om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    om.registerModule(new JodaModule());
    startTime = DateTime.now();
  }

  @Override
  public void process(String line) {
    lastRecord = parseRecord(line);
    if (lastRecord == null) {
      skippedRecords++;
    } else {
      try {
        lastRecordJson = om.writeValueAsString(lastRecord);
        // LOGGER.log(Level.INFO, "record " + lastRecordJson);
      } catch (JsonProcessingException e) {
        LOGGER.log(Level.SEVERE, "unable to write json for " + lastRecord);
      }
      numRecords++;
    }
  }

  private Pattern pattern = Pattern
      .compile("(?<ip>\\w+(\\.\\w+)*)\\s\\-\\s\\-\\s\\[(?<ts>.*)\\]\\s\\\"(?<req>.*)\\\"\\s(?<code>\\d+)\\s(?<bytes>(\\d+|\\-))");

  // 01/Jul/1995:00:00:09 -0400
  private DateTimeFormatter dtf = DateTimeFormat
      .forPattern("dd/MMM/YYYY:HH:mm:ss Z");

  protected Record parseRecord(String line) {
    Matcher m = pattern.matcher(line);
    if (!m.find()) {
      LOGGER.log(Level.SEVERE, "unable to parse:" + line);
      return null;
    }
    Record rec = new Record();
    try {
      rec.setHost(m.group("ip"));
      DateTime t = startTime.plusSeconds((int)(Math.random() * 10));
      rec.setTimestamp(t);
      rec.setRequest(m.group("req"));
      rec.setReplyCode(Integer.parseInt(m.group("code")));
      if (m.group("bytes").equals("-")) {
        rec.setReplyBytes(0);
      } else {
        rec.setReplyBytes(Integer.parseInt(m.group("bytes")));
      }
    } catch (RuntimeException e) {
      LOGGER.log(Level.SEVERE,
          "unable to parse " + line + " due to " + e.getMessage());
      return null;
    }
    return rec;
  }

  public long getSkippedRecords() {
    return skippedRecords;
  }

  @Override
  public void start() {
  }

  @Override
  public void end() {
    LOGGER.log(Level.INFO, "records = " + numRecords + ", skipped = "
        + skippedRecords);
  }

  @Override
  public long records() {
    return numRecords + skippedRecords;
  }

  public long getNumRecords() {
    return numRecords;
  }

  public Record getLastRecord() {
    return lastRecord;
  }
}
