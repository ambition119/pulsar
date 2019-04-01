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
import akka.actor.ActorSelection;
import akka.actor.UntypedAbstractActor;
import java.util.Optional;
import org.apache.pulsar.io.akka.AkkaAbstractSource.AkkaRecord;

/**
 * Generalized receiver actor which receives messages
 * from the feeder or publisher actor.
 */
public class ReceiverActor extends UntypedAbstractActor {

  // --- Fields set by the constructor
  private final AkkaAbstractSource akkaAbstractSource;
  private final String urlOfFeeder;
  private final boolean autoAck;

  // --- Runtime fields
  private ActorSelection remotePublisher;

  public ReceiverActor(AkkaAbstractSource akkaAbstractSource, String urlOfFeeder, boolean autoAck) {
    this.akkaAbstractSource = akkaAbstractSource;
    this.urlOfFeeder = urlOfFeeder;
    this.autoAck = autoAck;
  }

  @Override
  public void preStart() throws Exception {
    remotePublisher = getContext().actorSelection(urlOfFeeder);
    remotePublisher.tell(new SubscribeReceiver(getSelf()), getSelf());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void onReceive(Object message) throws Exception {
    AkkaRecord akkaRecord = new AkkaRecord<>(Optional.ofNullable(""), akkaAbstractSource.extractValue(message));
    akkaAbstractSource.consume(akkaRecord);
    if (autoAck) {
      getSender().tell("ack", getSelf());
    }
  }

  @Override
  public void postStop() throws Exception {
    remotePublisher.tell(new UnsubscribeReceiver(ActorRef.noSender()),
        ActorRef.noSender());
  }
}
