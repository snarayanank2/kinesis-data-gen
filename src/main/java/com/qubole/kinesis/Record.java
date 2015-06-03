package com.qubole.kinesis;

import org.joda.time.DateTime;

public class Record {
  private String host;
  private DateTime timestamp;
  private String request;
  private int replyCode;
  private int replyBytes;

  public Record() {
  }

  public Record(String host, DateTime timestamp, String request, int replyCode,
      int replyBytes) {
    this.host = host;
    this.timestamp = timestamp;
    this.request = request;
    this.replyCode = replyCode;
    this.replyBytes = replyBytes;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public DateTime getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(DateTime timestamp) {
    this.timestamp = timestamp;
  }

  public String getRequest() {
    return request;
  }

  public void setRequest(String request) {
    this.request = request;
  }

  public int getReplyCode() {
    return replyCode;
  }

  public void setReplyCode(int replyCode) {
    this.replyCode = replyCode;
  }

  public int getReplyBytes() {
    return replyBytes;
  }

  public void setReplyBytes(int replyBytes) {
    this.replyBytes = replyBytes;
  }

  @Override
  public String toString() {
    return "Record [host=" + host + ", timestamp=" + timestamp + ", request="
        + request + ", replyCode=" + replyCode + ", replyBytes=" + replyBytes
        + "]";
  }
}
