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
package org.apache.pulsar.io.akka;

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

/**
 * Akka Source Connector Config.
 */
@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class AkkaSourceConfig implements Serializable {
  private static final long serialVersionUID = 5442725163644124047L;

  @FieldDoc(
      defaultValue = "receiver-actor-system",
      help = "The network protocol to use, supported values are 'tcp', 'udp', and 'http'")
  private String receiverActorName;

  @FieldDoc(
      required = true,
      defaultValue = "",
      help = "The network protocol to use, supported values are 'tcp', 'udp', and 'http'")
  private String feederActorName;

  @FieldDoc(
      defaultValue = "",
      help = "akka system configuration file")
  private String akkaConfigResources;

  @FieldDoc(
      required = true,
      defaultValue = "",
      help = "The network protocol to use, supported values are 'tcp', 'udp', and 'http'")
  private String urlOfFeeder;

  @FieldDoc(
      defaultValue = "",
      help = "The network protocol to use, supported values are 'tcp', 'udp', and 'http'")
  private String provider;

  @FieldDoc(
      defaultValue = "",
      help = "The network protocol to use, supported values are 'tcp', 'udp', and 'http'")
  private String enabledTransports;

  public static AkkaSourceConfig load(Map<String, Object> map) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(new ObjectMapper().writeValueAsString(map), AkkaSourceConfig.class);
  }

  public static AkkaSourceConfig load(String yamlFile) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    return mapper.readValue(new File(yamlFile), AkkaSourceConfig.class);
  }
}
