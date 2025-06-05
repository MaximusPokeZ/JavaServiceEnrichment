package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;

@Slf4j
public class ServiceEnrichment implements Service {
    @Override
    public void start(Config config) {
        try {
            KafkaReader kafkaReader = new KafkaReaderImpl(config);
            kafkaReader.processing();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
}