/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.influxdb;

import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

/**
 * A Simple hbase sink, which interprets input Record in generic record.
 */
@Connector(
    name = "InfluxDB",
    type = IOType.SINK,
    help = "The InfluxDBSink is used for moving messages from Pulsar to InfluxDB.",
    configClass = InfluxDBSinkConfig.class
)
@Slf4j
public class InfluxDBSink implements Sink<InfluxDBPoint> {

  private static final TimeUnit flushDurationTimeUnit = TimeUnit.MILLISECONDS;;
  private transient InfluxDB influxDBClient;
  private final InfluxDBSinkConfig influxDBSinkConfig;

  /**
   * Creates a new {@link InfluxDBSink} that connects to the InfluxDB server.
   *
   * @param influxDBSinkConfig The configuration of {@link InfluxDBSinkConfig}
   */
  public InfluxDBSink(InfluxDBSinkConfig influxDBSinkConfig) {
    this.influxDBSinkConfig = Preconditions.checkNotNull(influxDBSinkConfig, "InfluxDB client config should not be null");
  }

  @Override
  public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
    influxDBClient = InfluxDBFactory
        .connect(influxDBSinkConfig.getUrl(), influxDBSinkConfig.getUsername(), influxDBSinkConfig.getPassword());

    if (!influxDBClient.databaseExists(influxDBSinkConfig.getDatabase())) {
      if(influxDBSinkConfig.isCreateDatabase()) {
        influxDBClient.createDatabase(influxDBSinkConfig.getDatabase());
      }
      else {
        throw new RuntimeException("This " + influxDBSinkConfig.getDatabase() + " database does not exist!");
      }
    }

    influxDBClient.setDatabase(influxDBSinkConfig.getDatabase());

    if (influxDBSinkConfig.getBatchActions() > 0) {
      influxDBClient.enableBatch(influxDBSinkConfig.getBatchActions(), influxDBSinkConfig.getFlushDuration(), flushDurationTimeUnit);
    }

    if (influxDBSinkConfig.isEnableGzip()) {
      influxDBClient.enableGzip();
    }
  }

  @Override
  public void write(Record<InfluxDBPoint> record) throws Exception {
    InfluxDBPoint dataPoint = record.getValue();
    if (isNullOrWhitespaceOnly(dataPoint.getMeasurement())) {
      throw new RuntimeException("No measurement defined");
    }

    Point.Builder builder = Point.measurement(dataPoint.getMeasurement())
        .time(dataPoint.getTimestamp(), TimeUnit.MILLISECONDS);

    if (dataPoint.getFields() == null || dataPoint.getFields().isEmpty()) {
      builder.fields(dataPoint.getFields());
    }

    if (dataPoint.getTags() == null || dataPoint.getTags().isEmpty()) {
      builder.tag(dataPoint.getTags());
    }

    Point point = builder.build();
    influxDBClient.write(point);
  }

  @Override
  public void close() throws Exception {
    if (influxDBClient.isBatchEnabled()) {
      influxDBClient.disableBatch();
    }
    influxDBClient.close();
  }

  private static boolean isNullOrWhitespaceOnly(String str) {
    if (str == null || str.length() == 0) {
      return true;
    }

    final int len = str.length();
    for (int i = 0; i < len; i++) {
      if (!Character.isWhitespace(str.charAt(i))) {
        return false;
      }
    }
    return true;
  }
}
