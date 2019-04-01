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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.pulsar.io.core.annotations.FieldDoc;

@Data
@Setter
@Getter
@EqualsAndHashCode(callSuper = false)
@ToString
@Accessors(chain = true)
public class InfluxDBSinkConfig implements Serializable {
  private static final long serialVersionUID = 4013162911059315334L;

  @FieldDoc(
      required = true,
      defaultValue = "",
      help = "the url to connect to InfluxDB.")
  private String url;

  @FieldDoc(
      required = true,
      defaultValue = "",
      help = "the username which is used to authorize against the influxDB instance.")
  private String username;

  @FieldDoc(
      required = true,
      defaultValue = "",
      help = "the password for the username which is used to authorize against the influxDB instance.")
  private String password;

  @FieldDoc(
      required = true,
      defaultValue = "",
      help = "the name of the database to write.")
  private String database;

  @FieldDoc(
      defaultValue = "2000",
      help = "number of Points written after which a write must happen.")
  private int batchActions = 2000;

  @FieldDoc(
      defaultValue = "100",
      help = "number of Points written after which a write must happen.")
  private int flushDuration = 100;

  @FieldDoc(
      defaultValue = "false",
      help = "the enableGzip value.")
  private boolean enableGzip = false;

  @FieldDoc(
      defaultValue = "false",
      help = "createDatabase switch value.")
  private boolean createDatabase = false;

  public static InfluxDBSinkConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), InfluxDBSinkConfig.class);
  }

  public static InfluxDBSinkConfig load(String yamlFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(yamlFile), InfluxDBSinkConfig.class);
  }
}
