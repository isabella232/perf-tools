/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.cdf.benchmarking;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.BatchingSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import org.threeten.bp.Instant;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Publishes messages of sizes 1, 10, 100 KB to a Pub/Sub Topic at rates of 1000, 2000 & 5000
 * messages/sec.
 */
public class PubSubPublisher {
  private static final Logger LOG = LoggerFactory.getLogger(PubSubPublisher.class);
  
  // no of messages to publish in one batch
  private static final long elementCountThreshold = 1_000;
  
  // batch delay threshold in seconds
  private static final int delayThreshold = 1;
  
  // byte threshold for each batch
  private static long byteThreshold;
  
  // delay time to slow down the publish operations
  private static long manualDelay;
  
  // message to publish
  private static String message;
  
  // no of threads to create
  private static int numThreads;
  
  public static void main(String[] args) {
    String projectId;
    String topicId;
    String publishRate;
    String eventSize;
    
    try {
      projectId = args[0];
      topicId = args[1];
      publishRate = args[2];
      eventSize = args[3];
    } catch (Exception e) {
      LOG.error(
          "Pass arguments for the project ID, topic ID, publish rate & event size (in that order)");
      throw new IllegalArgumentException(
          "Pass arguments for the project ID, topic ID & publish rate & event size (in that order)");
    }
    
    InputStream inputStream = validateAndConfigureSettings(publishRate, eventSize);
    
    assert inputStream != null;
    Scanner scanner = new Scanner(inputStream, StandardCharsets.UTF_8.name());
    message = scanner.useDelimiter("\\A").next();
    
    LOG.info(
        "Spinning up {} thread(s) to publish messages at a rate of {} messages/sec",
        numThreads,
        publishRate);
    ExecutorService service = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      service.submit(new PublishMessages(projectId, topicId));
    }
  }
  
  /**
   * Validates the publish rate and event size arguments and configures other settings required to
   * publish messages based on these arguments.
   *
   * @param publishRate Rate at which messages should be published to the Pub/Sub Topic
   * @param eventSize   Size of the message to publish to the Pub/Sub Topic
   * @return An {@link InputStream} of the message to be published
   */
  private static InputStream validateAndConfigureSettings(String publishRate, String eventSize) {
    InputStream inputStream;
    
    // determine number of threads based on the publish rate. A single thread can publish messages
    // at a rate of 1000 messages/sec
    switch (publishRate) {
      case "1000":
        numThreads = 1;
        break;
      case "2000":
        numThreads = 2;
        break;
      case "5000":
        numThreads = 5;
        break;
      default:
        LOG.error("Publish rate must be one of 1000, 2000 or 5000");
        throw new IllegalArgumentException("Publish rate must be one of 1000, 2000 or 5000");
    }
    
    // read file and set byte threshold based on event size
    switch (eventSize) {
      case "1":
        inputStream = PubSubPublisher.class.getClassLoader().getResourceAsStream("1kb.json");
        byteThreshold = 1_000_000;
        manualDelay = 980;
        break;
      case "10":
        inputStream = PubSubPublisher.class.getClassLoader().getResourceAsStream("10kb.json");
        byteThreshold = 10_000_000;
        manualDelay = 980;
        break;
      case "100":
        inputStream = PubSubPublisher.class.getClassLoader().getResourceAsStream("100kb.json");
        byteThreshold = 100_000;
        manualDelay = 1960;
        break;
      default:
        LOG.error("Event size must be one of 1, 10 or 100");
        throw new IllegalArgumentException("Event size must be one of 1, 10 or 100");
    }
    
    return inputStream;
  }
  
  /**
   * Publishes messages to a Pub/Sub Topic at a rate of 1000 messages/sec.
   */
  private static class PublishMessages implements Runnable {
    String projectId;
    String topicId;
    
    public PublishMessages(String projectId, String topicId) {
      this.projectId = projectId;
      this.topicId = topicId;
    }
    
    @Override
    public void run() {
      LOG.debug("Thread Name: " + Thread.currentThread().getName());
      
      TopicName topicName = TopicName.of(projectId, topicId);
      Publisher publisher;
      List<ApiFuture<String>> messageIdFutures;
      Instant startTime;
      Instant endTime;
      
      try {
        BatchingSettings batchingSettings =
            BatchingSettings.newBuilder()
                .setElementCountThreshold(elementCountThreshold)
                .setRequestByteThreshold(byteThreshold)
                .setDelayThreshold(Duration.ofSeconds(delayThreshold))
                .build();
        
        publisher = Publisher.newBuilder(topicName).setBatchingSettings(batchingSettings).build();
        
        ByteString byteString = ByteString.copyFromUtf8(message);
        PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(byteString).build();
        
        while (true) {
          messageIdFutures = new ArrayList<>();
          
          startTime = Instant.now();
          for (int i = 0; i < 1000; i++) {
            ApiFuture<String> messageId = publisher.publish(pubsubMessage);
            messageIdFutures.add(messageId);
          }
          
          // pause the thread to slow down the publish operations to 1000 messages/sec
          TimeUnit.MILLISECONDS.sleep(manualDelay);
          endTime = Instant.now();
          
          LOG.debug(
              "Published {} messages in {}",
              messageIdFutures.size(),
              Duration.between(startTime, endTime));
        }
      } catch (Exception e) {
        LOG.error("Could not publish messages to Pub/Sub", e);
      }
    }
  }
}
