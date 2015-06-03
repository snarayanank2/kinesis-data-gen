package com.qubole.kinesis;

import java.io.InputStream;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;

public class RecordReaderTest {
  private final static Logger LOGGER = Logger.getLogger(RecordReaderTest.class
      .getName());

  @Test
  public void testRecordParsing() {
    URL u = this.getClass().getResource("/");
    LOGGER.log(Level.INFO, "url " + u.getPath() + " " + u.toExternalForm());
    InputStream is = this.getClass().getResourceAsStream(
        "/sample_data.txt");
    RecordReader rr = new RecordReader(is);
    while (rr.hasNext()) {
      Record rec = rr.next();
      LOGGER.log(Level.INFO, "record " + rec);
    }
  }
}
