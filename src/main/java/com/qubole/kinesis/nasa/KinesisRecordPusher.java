package com.qubole.kinesis.nasa;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;

public class KinesisRecordPusher extends RecordParser {
  private final static Logger LOGGER = Logger.getLogger(KinesisRecordPusher.class
      .getName());

  private String lastPushedRecordJson;
  private AmazonKinesisClient client;
  private String stream;
  private List<PutRecordsRequestEntry> requestEntries;
  private int MAX_ENTRIES=400;
  
  public KinesisRecordPusher(AmazonKinesisClient client, String stream) {
    this.client = client;
    this.stream = stream;
    this.requestEntries = new ArrayList<PutRecordsRequestEntry>(MAX_ENTRIES);
  }
  
  @Override
  public void start() {
    
  }
  
  @Override
  public void end() {
    if (requestEntries.size() > 0) {
      pushToKinesis();
    }
  }
  
  private void pushToKinesis() {
    try {
      PutRecordsRequest request = new PutRecordsRequest();
      request.setRecords(requestEntries);
      request.setStreamName(stream);
      PutRecordsResult putRecordsResult = client.putRecords(request);
      if (putRecordsResult.getFailedRecordCount() > 0) {
        LOGGER.log(Level.INFO,
            "Failed record count " + putRecordsResult.getFailedRecordCount());
      }
      requestEntries.clear();
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "hit problem", e.getMessage());
    }
  }

  @Override
  public void process(String line) {
    super.process(line);
    if (lastPushedRecordJson != lastRecordJson) {
      String partitionKey = lastRecord.getHost();
      PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
      entry.setData(ByteBuffer.wrap(lastRecordJson.getBytes()));
      entry.setPartitionKey(partitionKey);
      requestEntries.add(entry);
      if (requestEntries.size() == MAX_ENTRIES) {
        pushToKinesis();
      }
    }
    lastPushedRecordJson = lastRecordJson;
  }
}
