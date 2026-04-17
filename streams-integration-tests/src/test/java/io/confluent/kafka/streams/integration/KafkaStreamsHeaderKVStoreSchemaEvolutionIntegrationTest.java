/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.streams.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Bytes;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for schema evolution with header-based KV state stores.
 *
 * <p>Tests validate that Kafka Streams header-aware state stores correctly handle
 * Avro schema evolution for both keys and values when using {@link HeaderSchemaIdSerializer}
 * (schema ID transported in record headers, not as a prefix in serialized bytes).
 *
 * <p>Context: Before implementing versioned state stores with header-based SR format,
 * we need to understand schema evolution behavior on existing header-based KV stores,
 * particularly for key evolution where byte representation changes can break lookups.
 */
public class KafkaStreamsHeaderKVStoreSchemaEvolutionIntegrationTest extends ClusterTestHarness {

  private static final String STORE_NAME = "schema-evolution-store";

  // --- Key schemas ---
  private static final Schema KEY_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"}"
          + "]"
          + "}");

  private static final Schema KEY_SCHEMA_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorKey\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"sensorId\",\"type\":\"string\"},"
          + "  {\"name\":\"region\",\"type\":\"string\",\"default\":\"us-east\"}"
          + "]"
          + "}");

  // --- Value schemas ---
  private static final Schema VALUE_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}");

  private static final Schema VALUE_SCHEMA_V2 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"},"
          + "  {\"name\":\"humidity\",\"type\":\"double\",\"default\":0.0}"
          + "]"
          + "}");

  public KafkaStreamsHeaderKVStoreSchemaEvolutionIntegrationTest() {
    super(1, true);
  }

  /**
   * Tests that value schema evolution works correctly with header-based KV stores.
   *
   * <p>Scenario: A KTable is backed by a header-aware state store. Records are first produced
   * with value schema v1, then the value schema is evolved to v2 (adding a field with default).
   * The test verifies that:
   * <ul>
   *   <li>Old values (written with v1) are still readable after schema evolution</li>
   *   <li>New values (written with v2) are stored and readable</li>
   *   <li>The store correctly handles mixed schema versions via iterators</li>
   * </ul>
   */
  @Test
  public void shouldReadOldValuesAfterValueSchemaEvolution() throws Exception {
    String inputTopic = "value-evolution-input";
    String appId = "value-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      // Produce records with value schema v1
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord key1 = newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-1");
        GenericRecord val1 = newRecord(VALUE_SCHEMA_V1,
            "temperature", 35.5, "timestamp", 1000L);

        GenericRecord key2 = newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-2");
        GenericRecord val2 = newRecord(VALUE_SCHEMA_V1,
            "temperature", 22.0, "timestamp", 2000L);

        producer.send(new ProducerRecord<>(inputTopic, key1, val1)).get();
        producer.send(new ProducerRecord<>(inputTopic, key2, val2)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 2);

      // Produce records with evolved value schema v2
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        // Update sensor-1 with v2 schema (includes humidity)
        GenericRecord key1 = newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-1");
        GenericRecord val1v2 = newRecord(VALUE_SCHEMA_V2,
            "temperature", 36.0, "timestamp", 3000L, "humidity", 65.0);

        // Add a new sensor with v2 schema
        GenericRecord key3 = newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-3");
        GenericRecord val3 = newRecord(VALUE_SCHEMA_V2,
            "temperature", 28.0, "timestamp", 4000L, "humidity", 70.0);

        producer.send(new ProducerRecord<>(inputTopic, key1, val1v2)).get();
        producer.send(new ProducerRecord<>(inputTopic, key3, val3)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 3);

      ReadOnlyKeyValueStore<GenericRecord, ValueAndTimestamp<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore()));

      // sensor-1 should have the updated v2 value
      ValueAndTimestamp<GenericRecord> result1 =
          store.get(newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-1"));
      assertNotNull(result1, "sensor-1 should be in the store");
      assertEquals(36.0, result1.value().get("temperature"));

      // sensor-2 should still have the old v1 value (readable after evolution)
      ValueAndTimestamp<GenericRecord> result2 =
          store.get(newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-2"));
      assertNotNull(result2, "sensor-2 should still be readable (value written with v1)");
      assertEquals(22.0, result2.value().get("temperature"));

      // sensor-3 should have the v2 value
      ValueAndTimestamp<GenericRecord> result3 =
          store.get(newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-3"));
      assertNotNull(result3, "sensor-3 should be in the store");
      assertEquals(28.0, result3.value().get("temperature"));

      int count = 0;
      try (KeyValueIterator<GenericRecord, ValueAndTimestamp<GenericRecord>> iter = store.all()) {
        while (iter.hasNext()) {
          iter.next();
          count++;
        }
      }
      assertEquals(3, count, "Store should contain exactly 3 entries");

    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Test that producing the same logical key under an evolved key schema (v1 → v2,
   * adding a field with default) creates a second store row instead of overwriting the first:
   * v2's {@code get()} returns the v2 row, v1's {@code get()} still returns the v1 row, and
   * {@code all()} yields both.
   */
  @Test
  public void shouldStoreSameLogicalKeyAsTwoRowsAfterKeySchemaEvolution() throws Exception {
    String inputTopic = "key-evolution-input";
    String appId = "key-evolution-test-" + System.currentTimeMillis();

    KafkaStreams streams = null;
    try {
      streams = startTableApp(inputTopic, appId);

      // Produce records with key schema v1
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord keyV1 = newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-1");
        GenericRecord val = newRecord(VALUE_SCHEMA_V1,
            "temperature", 35.5, "timestamp", 1000L);

        producer.send(new ProducerRecord<>(inputTopic, keyV1, val)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 1);

      ReadOnlyKeyValueStore<GenericRecord, ValueAndTimestamp<GenericRecord>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore()));

      // Lookup with v1-serialized key works
      ValueAndTimestamp<GenericRecord> resultV1 =
          store.get(newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-1"));
      assertNotNull(resultV1, "Lookup with v1 key should find the entry");
      assertEquals(35.5, resultV1.value().get("temperature"));

      // Produce the same logical key with evolved key schema v2
      // v2 adds "region" field with default "us-east" — this changes the Avro binary
      try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
        GenericRecord keyV2 = newRecord(KEY_SCHEMA_V2,
            "sensorId", "sensor-1", "region", "us-east");
        GenericRecord val = newRecord(VALUE_SCHEMA_V1,
            "temperature", 40.0, "timestamp", 2000L);

        producer.send(new ProducerRecord<>(inputTopic, keyV2, val)).get();
        producer.flush();
      }

      waitForStoreToContainKeys(streams, 2);

      // Lookup with v2 key finds ONLY the new entry (different byte representation)
      ValueAndTimestamp<GenericRecord> resultV2 =
          store.get(newRecord(KEY_SCHEMA_V2, "sensorId", "sensor-1", "region", "us-east"));
      assertNotNull(resultV2, "Lookup with v2 key should find the new entry");
      assertEquals(40.0, resultV2.value().get("temperature"),
          "v2 key lookup should return the value produced with v2 key");

      // Lookup with v1 key STILL finds the old entry (it was not overwritten)
      ValueAndTimestamp<GenericRecord> resultV1Again =
          store.get(newRecord(KEY_SCHEMA_V1, "sensorId", "sensor-1"));
      assertNotNull(resultV1Again,
          "v1 key entry should still exist — v2 key has different bytes, so it did not overwrite");
      assertEquals(35.5, resultV1Again.value().get("temperature"),
          "Old entry should retain original value");

      // Iterator shows both entries (duplicate logical key, different byte keys)
      List<String> sensorIds = new ArrayList<>();
      List<Double> temperatures = new ArrayList<>();
      try (KeyValueIterator<GenericRecord, ValueAndTimestamp<GenericRecord>> iter = store.all()) {
        while (iter.hasNext()) {
          KeyValue<GenericRecord, ValueAndTimestamp<GenericRecord>> entry = iter.next();
          sensorIds.add(entry.key.get("sensorId").toString());
          temperatures.add((double) entry.value.value().get("temperature"));
        }
      }
      assertEquals(2, sensorIds.size(),
          "Store should have 2 entries: same logical key 'sensor-1' stored twice "
              + "with different byte representations (v1 and v2 key schemas)");
      assertTrue(
          sensorIds.stream().allMatch("sensor-1"::equals),
          "Both store rows should belong to logical key 'sensor-1', but got: " + sensorIds);
      assertEquals(
          new HashSet<>(Arrays.asList(35.5, 40.0)),
          new HashSet<>(temperatures),
          "The two rows should hold the v1-written and v2-written values (35.5 and 40.0)");

    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Creates {@code inputTopic}, builds a KTable materialized into a header-aware state store
   * named {@link #STORE_NAME}, and starts the Streams app under {@code appId}.
   */
  private KafkaStreams startTableApp(String inputTopic, String appId) throws Exception {
    createTopics(inputTopic);
    StreamsBuilder builder = new StreamsBuilder();
    builder.table(
        inputTopic,
        Consumed.with(createKeySerde(), createValueSerde()),
        Materialized.<GenericRecord, GenericRecord, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
            .withKeySerde(createKeySerde())
            .withValueSerde(createValueSerde()));
    return startStreams(builder, appId);
  }

  private void createTopics(String... topics) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin
          .createTopics(
              Arrays.stream(topics)
                  .map(t -> new NewTopic(t, 1, (short) 1))
                  .collect(Collectors.toList()))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
  }

  private GenericAvroSerde createKeySerde() {
    GenericAvroSerde serde = new GenericAvroSerde();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, true);
    return serde;
  }

  private GenericAvroSerde createValueSerde() {
    GenericAvroSerde serde = new GenericAvroSerde();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, false);
    return serde;
  }

  private KafkaProducer<GenericRecord, GenericRecord> createHeaderProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    props.put(
        AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    props.put(
        AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return new KafkaProducer<>(props);
  }

  private KafkaStreams startStreams(StreamsBuilder builder, String appId) throws Exception {
    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    streamsProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    // Enable header-aware state stores
    streamsProps.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, "HEADERS");

    CountDownLatch startedLatch = new CountDownLatch(1);
    KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
    streams.setStateListener(
        (newState, oldState) -> {
          if (newState == KafkaStreams.State.RUNNING) {
            startedLatch.countDown();
          }
        });
    streams.start();
    assertTrue(
        startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
    return streams;
  }

  private void waitForStoreToContainKeys(KafkaStreams streams, int expectedCount)
      throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        ReadOnlyKeyValueStore<GenericRecord, ValueAndTimestamp<GenericRecord>> store =
            streams.store(StoreQueryParameters.fromNameAndType(
                STORE_NAME, QueryableStoreTypes.timestampedKeyValueStore()));
        int count = 0;
        try (KeyValueIterator<GenericRecord, ValueAndTimestamp<GenericRecord>> iter = store.all()) {
          while (iter.hasNext()) {
            iter.next();
            count++;
          }
        }
        if (count >= expectedCount) {
          return;
        }
      } catch (Exception e) {
        // Store may not be ready yet
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "Store did not contain " + expectedCount + " entries within timeout");
  }

  private static GenericRecord newRecord(Schema schema, Object... fieldValues) {
    GenericRecord record = new GenericData.Record(schema);
    for (int i = 0; i < fieldValues.length; i += 2) {
      record.put((String) fieldValues[i], fieldValues[i + 1]);
    }
    return record;
  }
}
