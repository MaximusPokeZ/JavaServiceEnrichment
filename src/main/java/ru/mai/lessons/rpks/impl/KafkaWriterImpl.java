package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Properties;

@Slf4j
public class KafkaWriterImpl implements KafkaWriter {
  private final String topic;

  private final KafkaProducer<String, String> producer;

  public KafkaWriterImpl(Config config) {
    Properties props = new Properties();
    props.put("bootstrap.servers", config.getString("kafka.producer.bootstrap.servers"));
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    this.producer = new KafkaProducer<>(props);

    this.topic = config.getString("kafka.producer.outputTopic");

    log.info("KafkaWriter has been created");
  }

  @Override
  public void processing(Message message) {
    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, message.getValue());

    producer.send(producerRecord, (metaRecord, exception) -> {
      if (exception != null) {
        log.error("Error from producer: {}", exception.getMessage(), exception);
      } else {
        log.info("Message sent. Offset: {}, Partition: {}", metaRecord.offset(), metaRecord.partition());
      }
    });
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
      log.info("KafkaWriter has been closed");
    }
  }

}
