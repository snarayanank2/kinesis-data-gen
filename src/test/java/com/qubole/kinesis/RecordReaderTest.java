package com.qubole.kinesis;

import java.io.InputStream;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.junit.Test;

public class RecordReaderTest {
  private final static Logger LOGGER = Logger.getLogger(RecordReaderTest.class
      .getName());

  @Test
  public void testRecordParsing() {
    URL u = this.getClass().getResource("/");
    LOGGER.log(Level.INFO, "url " + u.getPath() + " " + u.toExternalForm());
    InputStream is = this.getClass().getResourceAsStream("/sample_data.txt");
    RecordReader rr = new RecordReader(is);
    int records = 0;
    while (rr.hasNext()) {
      Record rec = rr.next();
      Assert.assertNotNull(rec);
      Assert.assertNotNull(rec.getTimestamp());
      Assert.assertNotNull(rec.getHost());
      Assert.assertNotNull(rec.getRequest());
      records++;
      LOGGER.log(Level.INFO, "record " + rec);
    }
    Assert.assertEquals("unexpected number of records", 7, records);
    Assert.assertEquals("record counting is off", 7, rr.getNumRecords());
    Assert.assertEquals("skipped record counting is off", 1, rr.getSkippedRecords());
  }
}
