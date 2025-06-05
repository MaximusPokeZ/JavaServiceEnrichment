package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import ru.mai.lessons.rpks.DbReader;
import ru.mai.lessons.rpks.model.Rule;

import javax.sql.DataSource;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class DbReaderImpl implements DbReader {
  private final DSLContext dslContext;
  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

  private final AtomicReference<Rule[]> rulesRef = new AtomicReference<>();

  public DbReaderImpl(Config config) {
    DataSource dataSource = createDataSource(config);
    this.dslContext = DSL.using(dataSource, SQLDialect.POSTGRES);

    int updateInterval = config.getInt("application.updateIntervalSec");
    executor.scheduleAtFixedRate(this::updateRules, 0, updateInterval, TimeUnit.SECONDS);
  }

  private DataSource createDataSource(Config config) {
    log.info("Creating datasource...");
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(config.getString("db.jdbcUrl"));
    hikariConfig.setUsername(config.getString("db.user"));
    hikariConfig.setPassword(config.getString("db.password"));
    hikariConfig.setDriverClassName(config.getString("db.driver"));
    return new HikariDataSource(hikariConfig);
  }

  private void updateRules() {
    try {
      log.info("Updating rules from DB...");
      Rule[] newRules = fetchRulesFromDatabase();
      this.rulesRef.set(newRules);
      log.info("Rules updated. Count: {}", newRules.length);
    } catch (Exception e) {
      log.error("Error updating rules", e);
    }
  }

  @SneakyThrows
  private Rule[] fetchRulesFromDatabase() {
    log.info("Fetching rules from DB...");
    return dslContext.select()
            .from("enrichment_rules")
            .fetch()
            .map(dbRecord -> new Rule(
                    dbRecord.get("enrichment_id", Long.class),
                    dbRecord.get("rule_id", Long.class),
                    dbRecord.get("field_name", String.class),
                    dbRecord.get("field_name_enrichment", String.class),
                    dbRecord.get("field_value", String.class),
                    dbRecord.get("field_value_default", String.class)
            ))
            .toArray(Rule[]::new);
  }

  @Override
  public Rule[] readRulesFromDB() {
    return rulesRef.get();
  }

  public void shutdown() {
    executor.shutdown();
    log.info("Shutting down database...");
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
      log.error("Error shutting down database", e);
    }
  }

  @Override
  public void close() {
    shutdown();
  }
}
