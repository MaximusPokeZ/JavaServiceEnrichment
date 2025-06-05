package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaReaderImpl implements KafkaReader {
  private final String topic;

  private final KafkaConsumer<String, String> consumer;
  private final KafkaWriter writer;

  private final DbReader dbReader;
  private final RuleProcessor ruleProcessor;

  public KafkaReaderImpl(Config config) {
    Properties props = new Properties();
    props.put("bootstrap.servers", config.getString("kafka.consumer.bootstrap.servers"));
    props.put("group.id", config.getString("kafka.consumer.group.id"));
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("auto.offset.reset", config.getString("kafka.consumer.auto.offset.reset"));

    this.dbReader = new DbReaderImpl(config);
    this.consumer = new KafkaConsumer<>(props);
    this.ruleProcessor = new RuleProcessorImpl(config);
    this.writer = new KafkaWriterImpl(config);

    this.topic = config.getString("kafka.consumer.inputTopic");

    log.info("Reading kafka topic: {}", topic);
  }

  @Override
  public void processing() {
    try {
      consumer.subscribe(Collections.singletonList(topic));
      while (true) {
        log.info("Waiting for consumer to read messages...");
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        records.forEach(consumerRecord -> {
          try {
            Message message = Message.builder()
                    .value(consumerRecord.value())
                    .build();

            log.info("Received message: {}", consumerRecord.value());

            Rule[] currentRules = dbReader.readRulesFromDB();

            Message processedMessage = ruleProcessor.processing(message, currentRules);

            writer.processing(processedMessage);
          } catch (Exception e) {
            log.error(e.getMessage());
          }
        });
      }
    } finally {
      consumer.close();
      writer.close();
      dbReader.close();
      log.info("Closing kafka consumer and DB reader");
    }
  }
}
