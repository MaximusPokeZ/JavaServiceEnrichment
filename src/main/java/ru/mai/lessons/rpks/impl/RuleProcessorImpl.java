package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.typesafe.config.Config;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.RuleProcessor;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class RuleProcessorImpl implements RuleProcessor {
  private final MongoDBClientEnricher enricher;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public RuleProcessorImpl(Config config) {
    this.enricher = new MongoDBClientEnricherImpl(config);
  }

  @Override
  public Message processing(Message message, Rule[] rules) {
    try {
      ObjectNode json = (ObjectNode) objectMapper.readTree(message.getValue());

      Map<String, Rule> ruleMap = Arrays.stream(rules)
              .collect(Collectors.toMap(
                      Rule::getFieldName,
                      rule -> rule,
                      (oldRule, newRule) -> (oldRule.getRuleId() > newRule.getRuleId() ? oldRule : newRule)
              ));

      for (Rule rule : ruleMap.values()) {
        Message enrichedMessage = enricher.enrich(rule, message);

        JsonNode enrichedJson = objectMapper.readTree(enrichedMessage.getValue());
        json.set(rule.getFieldName(), enrichedJson.get(rule.getFieldName()));
      }

      return Message.builder()
              .value(objectMapper.writeValueAsString(json))
              .build();

    } catch (JsonProcessingException e) {
      log.error("Error during rule processing", e);
    }
    return null;
  }
}