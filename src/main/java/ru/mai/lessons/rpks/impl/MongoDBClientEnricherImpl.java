package ru.mai.lessons.rpks.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import ru.mai.lessons.rpks.MongoDBClientEnricher;
import ru.mai.lessons.rpks.model.Message;
import ru.mai.lessons.rpks.model.Rule;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

@Slf4j
public class MongoDBClientEnricherImpl implements MongoDBClientEnricher {
  private final MongoClient mongoClient;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final String collectionName;
  private final String databaseName;

  public MongoDBClientEnricherImpl(Config config) {
    String connectionString = config.getString("mongo.connectionString");
    this.databaseName = config.getString("mongo.database");
    this.collectionName = config.getString("mongo.collection");

    this.mongoClient = MongoClients.create(connectionString);
  }

  @Override
  public Message enrich(Rule rule, Message message) {
    String filterValue = rule.getFieldValue();
    log.info("Using filter value from rule: '{}'", filterValue);

    MongoDatabase db = mongoClient.getDatabase(databaseName);
    MongoCollection<Document> collection = db.getCollection(collectionName);

    List<Document> foundNeedDocs = collection
            .find(new Document(rule.getFieldNameEnrichment(), filterValue))
            .into(new ArrayList<>());

    if (foundNeedDocs.isEmpty()) {
      log.info("No matching documents found in MongoDB, message will be returned unchanged");
      return enrichWithValue(message, rule.getFieldName(), rule.getFieldValueDefault());
    }

    String enrichedDocJson = foundNeedDocs.stream()
            .max(Comparator.comparing(doc -> doc.getObjectId("_id")))
            .map(Document::toJson)
            .orElse(rule.getFieldValueDefault());

    log.info("Enrichment value from MongoDB: '{}'", enrichedDocJson);

    return enrichWithValue(message, rule.getFieldName(), enrichedDocJson);
  }


  private Message enrichWithValue(Message message, String fieldName, String value) {
    try {
      ObjectNode json = (ObjectNode) objectMapper.readTree(message.getValue());
      JsonNode valueNode = parseValueToJsonNode(value);

      json.set(fieldName, valueNode);
      return Message.builder()
              .value(objectMapper.writeValueAsString(json))
              .build();
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }
    return message;
  }

  private JsonNode parseValueToJsonNode(String value) {
    try {
      return objectMapper.readTree(value);
    } catch (JsonProcessingException e) {
      return new TextNode(value);
    }
  }

}
