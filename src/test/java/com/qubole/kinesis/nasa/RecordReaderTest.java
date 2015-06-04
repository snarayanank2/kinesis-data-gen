package com.qubole.kinesis.nasa;

import java.io.InputStream;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import junit.framework.Assert;

import org.junit.Test;

import com.qubole.kinesis.core.NullConsumer;
import com.qubole.kinesis.executor.StreamExperiment;
import com.qubole.kinesis.text.FileLineReader;

public class RecordReaderTest {
  private final static Logger LOGGER = Logger.getLogger(RecordReaderTest.class
      .getName());

  @Test
  public void testRecordParsing() {
    URL u = this.getClass().getResource("/");
    LOGGER.log(Level.INFO, "url " + u.getPath() + " " + u.toExternalForm());
    InputStream is = this.getClass().getResourceAsStream("/sample_data.txt");
    FileLineReader flr = new FileLineReader(is);
    RecordParser rp = new RecordParser();
    String line = null;
    while ( (line = flr.next()) != null) {
      rp.process(line);
      Record rec = rp.getLastRecord();
      if (rec != null) {
        Assert.assertNotNull(rec.getTimestamp());
        Assert.assertNotNull(rec.getHost());
        Assert.assertNotNull(rec.getRequest());
        LOGGER.log(Level.INFO, "record " + rec);
      }
    }
    Assert.assertEquals("unexpected number of produced records", 9, flr.records());
    Assert.assertEquals("unexpected number of consumed records", 9, rp.records());
    Assert.assertEquals("parsed record counting is off", 8, rp.getNumRecords());
    Assert.assertEquals("skipped record counting is off", 1, rp.getSkippedRecords());
  }
  
  @Test
  public void testProducerConsumer() throws InterruptedException, ExecutionException, TimeoutException {
    URL u = this.getClass().getResource("/");
    LOGGER.log(Level.INFO, "url " + u.getPath() + " " + u.toExternalForm());
    InputStream is = this.getClass().getResourceAsStream("/sample_data.txt");
    FileLineReader rr = new FileLineReader(is);
    NullConsumer<String> consumer = new NullConsumer<String>();
    StreamExperiment<String> experiment = new StreamExperiment<String>(3, rr, consumer);
    experiment.runExperiment();
    Assert.assertEquals("producer record counting is off", 9, rr.records());
    Assert.assertEquals("consumer record counting is off", 9, consumer.records());
  }

}
