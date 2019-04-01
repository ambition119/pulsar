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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import lombok.Data;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple Akka Source connector to listen for incoming messages and write to user-defined Pulsar topic.
 */
@Connector(
    name = "netty",
    type = IOType.SOURCE,
    help = "A simple Akka Source connector to listen for incoming messages and write to user-defined Pulsar topic",
    configClass = AkkaAbstractSource.class)
public abstract class AkkaAbstractSource<V> extends PushSource<V> {
  private static final Logger LOG = LoggerFactory.getLogger(AkkaAbstractSource.class);

  private String receiverActorName;
  private String feederActorName;
  private String urlOfFeeder;

  // --- Runtime fields
  private transient Class<?> classForActor;
  private transient ActorSystem receiverActorSystem;
  private transient ActorRef receiverActor;
  protected transient boolean autoAck;

  @Override
  public void open(Map<String, Object> config, SourceContext sourceContext) throws Exception {
    AkkaSourceConfig akkaSourceConfig = AkkaSourceConfig.load(config);
    this.receiverActorName = akkaSourceConfig.getReceiverActorName();
    this.feederActorName = akkaSourceConfig.getFeederActorName();
    this.urlOfFeeder = akkaSourceConfig.getUrlOfFeeder();

    Config finalAkkaConfig;
    if (null != akkaSourceConfig.getAkkaConfigResources()) {
      finalAkkaConfig = ConfigFactory.load(akkaSourceConfig.getAkkaConfigResources());
    } else {
      finalAkkaConfig = ConfigFactory.empty();

      if (null == akkaSourceConfig.getProvider()) {
        finalAkkaConfig = finalAkkaConfig.withValue("akka.actor.provider",
            ConfigValueFactory.fromAnyRef("akka.remote.RemoteActorRefProvider"));
      }
      if (null == akkaSourceConfig.getEnabledTransports()) {
        finalAkkaConfig = finalAkkaConfig.withValue("akka.remote.enabled-transports",
            ConfigValueFactory.fromAnyRef(Collections.singletonList("akka.remote.netty.tcp")));
      }
    }

    receiverActorSystem = ActorSystem.create(receiverActorName, finalAkkaConfig);
    LOG.info("Starting the Receiver actor {}", receiverActorName);
    this.classForActor = ReceiverActor.class;
    receiverActor = receiverActorSystem.actorOf(Props.create(classForActor, sourceContext, urlOfFeeder, autoAck),feederActorName);

    //TODO: 这个赋值
    if (finalAkkaConfig.hasPath("akka.remote.auto-ack") &&
        finalAkkaConfig.getString("akka.remote.auto-ack").equals("on")) {
      autoAck = true;
    } else {
      autoAck = false;
    }
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing source");
    if (receiverActorSystem != null) {
      receiverActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
      receiverActorSystem.terminate();
    }
  }

  public abstract V extractValue(Object message);

  @Data
  static public class AkkaRecord<V> implements Record<V>, Serializable {
    private final Optional<String> key;
    private final V value;
  }
}
