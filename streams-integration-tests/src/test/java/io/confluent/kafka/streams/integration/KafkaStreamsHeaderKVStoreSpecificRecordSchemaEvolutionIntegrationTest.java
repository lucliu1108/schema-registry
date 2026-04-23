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
import io.confluent.kafka.streams.integration.avro.SensorKey;
import io.confluent.kafka.streams.integration.avro.SensorReading;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for value-schema evolution on a header-based KV state store,
 * driven via Avro <em>SpecificRecord</em> generated classes (as opposed to
 * {@link GenericRecord}). This mirrors how production Kafka Streams applications
 * are typically written: the reader binds to a compiled class whose schema is
 * the reader schema.
 *
 * <p>Both tests frame schema evolution as a rolling upgrade with two independent
 * actors:
 * <ul>
 *   <li><b>writer</b> — the upstream producer writing into the input topic.
 *   <li><b>reader</b> — the Kafka Streams app consuming the input topic.
 * </ul>
 *
 * <p>Because the compatibility contract is BACKWARD, the reader must be upgraded
 * first; the writer may upgrade later (or never).
 */
public class KafkaStreamsHeaderKVStoreSpecificRecordSchemaEvolutionIntegrationTest
    extends ClusterTestHarness {

  private static final String STORE_NAME = "specific-record-evolution-store";

  /**
   * Writer-side v1 shape. Shares the full name
   * ({@code io.confluent.kafka.streams.integration.avro.SensorReading}) with the
   * generated {@link SensorReading} class so Avro can resolve v1-encoded bytes
   * through the v2 reader class, defaulting the missing {@code humidity} field.
   */
  private static final Schema VALUE_SCHEMA_V1 = new Schema.Parser().parse(
      "{"
          + "\"type\":\"record\","
          + "\"name\":\"SensorReading\","
          + "\"namespace\":\"io.confluent.kafka.streams.integration.avro\","
          + "\"fields\":["
          + "  {\"name\":\"temperature\",\"type\":\"double\"},"
          + "  {\"name\":\"timestamp\",\"type\":\"long\"}"
          + "]"
          + "}");

  public KafkaStreamsHeaderKVStoreSpecificRecordSchemaEvolutionIntegrationTest() {
    super(1, true);
  }

  /**
   * Reader-upgraded, writer still on v1.
   *
   * <p>The upstream producer writes v1 bytes (no {@code humidity}) into the input
   * topic <em>before</em> the reader app starts. The reader is already on v2 —
   * {@link SpecificAvroSerde}{@code <SensorReading>} — so v1 records must be
   * projected into the v2 class with {@code humidity} filled from its schema
   * default.
   */
  @Test
  public void shouldReadOldWriterDataAfterReaderUpgrade() throws Exception {
    String inputTopic = "reader-upgrade-input";
    String appId = "reader-upgrade-test-" + System.currentTimeMillis();

    createTopics(inputTopic);

    // Old (v1) writer populates the topic.
    try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
      producer.send(new ProducerRecord<>(inputTopic,
          sensorKeyAsGeneric("sensor-1"),
          v1Value(35.5, 1000L))).get();
      producer.send(new ProducerRecord<>(inputTopic,
          sensorKeyAsGeneric("sensor-2"),
          v1Value(22.0, 2000L))).get();
      producer.flush();
    }

    KafkaStreams streams = null;
    try {
      streams = startTableApp(appId, inputTopic, null);
      waitForStoreToContainKeys(streams, 2);

      ReadOnlyKeyValueStore<SensorKey, ValueTimestampHeaders<SensorReading>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      ValueTimestampHeaders<SensorReading> r1 = store.get(new SensorKey("sensor-1"));
      assertNotNull(r1, "sensor-1 should be readable through the upgraded v2 reader");
      assertEquals(35.5, r1.value().getTemperature());
      assertEquals(0.0, r1.value().getHumidity(),
          "humidity should fall back to the v2 schema default for v1-written bytes");

      // read through value schema v2
      ValueTimestampHeaders<SensorReading> r2 = store.get(new SensorKey("sensor-2"));
      assertNotNull(r2);
      assertTrue(SensorReading.getClassSchema().getField("humidity") != null,
          "reader class must be the v2 SensorReading (carries humidity)");
      assertEquals(22.0, r2.value().getTemperature());
      assertEquals(0.0, r2.value().getHumidity());
    } finally {
      if (streams != null) {
        streams.close(Duration.ofSeconds(10));
      }
    }
  }

  /**
   * Writer upgraded after the reader, with a reader-app restart in between.
   *
   * Step 1: old producer writes v1 data; v2 reader app consumes it into the state store.
   * Step 2: the streams app is shut down and its local state is wiped,
   * forcing the next run to rebuild from the changelog.
   * Step 3: the producer is upgraded to v2 and writes new records including {@code humidity};
   * Step 4: the streams app stup down and local state wiped again.
   * Step 5: a v1 producer is still writing v1 bytes after the writer upgrade.
   * the reader app comes back up under the same {@code application.id} and serves both v1-origin and v2-origin rows.
   */
  @Test
  public void shouldReadNewWriterDataAfterWriterUpgrade() throws Exception {
    String inputTopic = "writer-upgrade-input";
    String appId = "writer-upgrade-test-" + System.currentTimeMillis();
    Path stateDir = Files.createTempDirectory("kstreams-specific-");

    createTopics(inputTopic);

    // --- Step 1: v1 writer + v2 reader ---
    try (KafkaProducer<GenericRecord, GenericRecord> producer = createHeaderProducer()) {
      producer.send(new ProducerRecord<>(inputTopic,
          sensorKeyAsGeneric("sensor-1"), v1Value(35.5, 1000L))).get();
      producer.send(new ProducerRecord<>(inputTopic,
          sensorKeyAsGeneric("sensor-2"), v1Value(22.0, 2000L))).get();
      producer.flush();
    }

    KafkaStreams streams = startTableApp(appId, inputTopic, stateDir);
    try {
      waitForStoreToContainKeys(streams, 2);
    } finally {
      streams.close(Duration.ofSeconds(10));
    }

    // -- Step 2: Restart and wipe local state so the next run must rebuild from the changelog topic. ---
    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);

    // --- Step 3: producer is now upgraded to v2 and starts writing humidity. ---
    try (KafkaProducer<SensorKey, SensorReading> producer = createSpecificHeaderProducer()) {
      producer.send(new ProducerRecord<>(inputTopic,
          new SensorKey("sensor-1"),
          SensorReading.newBuilder()
              .setTemperature(36.0).setTimestamp(3000L).setHumidity(65.0).build())).get();
      producer.send(new ProducerRecord<>(inputTopic,
          new SensorKey("sensor-3"),
          SensorReading.newBuilder()
              .setTemperature(28.0).setTimestamp(4000L).setHumidity(70.0).build())).get();
      producer.flush();
    }

    streams = startTableApp(appId, inputTopic, stateDir);
    try {
      waitForStoreToContainKeys(streams, 3);

      ReadOnlyKeyValueStore<SensorKey, ValueTimestampHeaders<SensorReading>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // sensor-1: v1 value overwritten by a v2 value post-upgrade
      SensorReading r1 = store.get(new SensorKey("sensor-1")).value();
      assertNotNull(r1);
      assertEquals(36.0, r1.getTemperature());
      assertEquals(65.0, r1.getHumidity(), "sensor-1 carries the v2 humidity after overwrite");

      // sensor-2: only written by the v1 producer, has default humidity when read with v2 reader.
      SensorReading r2 = store.get(new SensorKey("sensor-2")).value();
      assertNotNull(r2, "sensor-2 should have been restored from the changelog");
      assertEquals(22.0, r2.getTemperature());
      assertEquals(0.0, r2.getHumidity(),
          "sensor-2 keeps the default humidity — its writer never upgraded");

      // sensor-3: written by v2 producer only.
      SensorReading r3 = store.get(new SensorKey("sensor-3")).value();
      assertNotNull(r3);
      assertEquals(28.0, r3.getTemperature());
      assertEquals(70.0, r3.getHumidity());

    } finally {
      streams.close(Duration.ofSeconds(10));
      deleteRecursively(stateDir);
    }

    // --- Step 4: second rolling bounce. Wipe local state again
    // so the next run rebuilds the store from the changelog. ---
    deleteRecursively(stateDir);
    Files.createDirectory(stateDir);

    // --- Step 5: an old producer on v1 keeps writing v1 bytes (overwrites sensor-3, adds sensor-4) ,
    //  the v2 reader still accepts these writes even after the writer rollout has begun. ---
    try (KafkaProducer<GenericRecord, GenericRecord> v1Producer = createHeaderProducer()) {
      v1Producer.send(new ProducerRecord<>(inputTopic,
          sensorKeyAsGeneric("sensor-3"), v1Value(40.0, 4500L))).get();
      v1Producer.send(new ProducerRecord<>(inputTopic,
          sensorKeyAsGeneric("sensor-4"), v1Value(45.0, 5000L))).get();
      v1Producer.flush();
    }

    streams = startTableApp(appId, inputTopic, stateDir);
    try {
      waitForStoreToContainKeys(streams, 4);

      ReadOnlyKeyValueStore<SensorKey, ValueTimestampHeaders<SensorReading>> store =
          streams.store(StoreQueryParameters.fromNameAndType(
              STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));

      // sensor-1: last write was the v2 overwrite in step 3.
      SensorReading r1 = store.get(new SensorKey("sensor-1")).value();
      assertNotNull(r1);
      assertEquals(36.0, r1.getTemperature());
      assertEquals(65.0, r1.getHumidity(), "sensor-1 carries the v2 humidity after overwrite");

      // sensor-2: untouched since step 1 — v1 only.
      SensorReading r2 = store.get(new SensorKey("sensor-2")).value();
      assertNotNull(r2, "sensor-2 should have been restored from the changelog");
      assertEquals(22.0, r2.getTemperature());
      assertEquals(0.0, r2.getHumidity(),
          "sensor-2 keeps the default humidity — its writer never upgraded");

      // sensor-3: v2 value from step 3 was overwritten by a v1 straggler in step 5.
      SensorReading r3 = store.get(new SensorKey("sensor-3")).value();
      assertNotNull(r3);
      assertEquals(40.0, r3.getTemperature());
      assertEquals(0.0, r3.getHumidity(),
          "sensor-3's latest writer is still on v1 — humidity falls back to the default");

      // sensor-4: brand new v1 write from the straggler producer.
      SensorReading r4 = store.get(new SensorKey("sensor-4")).value();
      assertNotNull(r4);
      assertEquals(45.0, r4.getTemperature());
      assertEquals(0.0, r4.getHumidity());
    } finally {
      streams.close(Duration.ofSeconds(10));
      deleteRecursively(stateDir);
    }
  }

  /**
   * Given a sensor ID, builds a GenericRecord for the key using the generated
   * SensorKey schema.
   */
  private GenericRecord sensorKeyAsGeneric(String sensorId) {

    return new GenericRecordBuilder(SensorKey.getClassSchema())
        .set("sensorId", sensorId).build();
  }

  private GenericRecord v1Value(double temperature, long timestamp) {
    return new GenericRecordBuilder(VALUE_SCHEMA_V1)
        .set("temperature", temperature)
        .set("timestamp", timestamp)
        .build();
  }

  /**
   * Builds a KTable materialized into a header-aware state store named
   * {@link #STORE_NAME}, then starts the Streams app under {@code appId}. If
   * {@code stateDir} is non-null, it's used as the Streams state directory so
   * tests can wipe it to force changelog restore.
   */
  private KafkaStreams startTableApp(String appId, String inputTopic, Path stateDir)
      throws Exception {
    StreamsBuilder builder = new StreamsBuilder();
    builder.table(
        inputTopic,
        Consumed.with(createKeySerde(), createValueSerde()),
        Materialized.<SensorKey, SensorReading>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME))
            .withKeySerde(createKeySerde())
            .withValueSerde(createValueSerde()));

    Properties streamsProps = new Properties();
    streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
    streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    streamsProps.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    if (stateDir != null) {
      streamsProps.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());
    }

    CountDownLatch started = new CountDownLatch(1);
    KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
    streams.setStateListener((newState, oldState) -> {
      if (newState == KafkaStreams.State.RUNNING) {
        started.countDown();
      }
    });
    streams.start();
    assertTrue(started.await(60, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING");
    return streams;
  }

  private SpecificAvroSerde<SensorKey> createKeySerde() {
    SpecificAvroSerde<SensorKey> serde = new SpecificAvroSerde<>();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, true);
    return serde;
  }

  private SpecificAvroSerde<SensorReading> createValueSerde() {
    SpecificAvroSerde<SensorReading> serde = new SpecificAvroSerde<>();
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    serde.configure(config, false);
    return serde;
  }

  /** Producer that ships schema IDs via record headers (generic/inline schemas). */
  private KafkaProducer<GenericRecord, GenericRecord> createHeaderProducer() {
    return new KafkaProducer<>(baseProducerProps());
  }

  /** Producer typed for SpecificRecord — used to simulate the upgraded writer. */
  private KafkaProducer<SensorKey, SensorReading> createSpecificHeaderProducer() {
    return new KafkaProducer<>(baseProducerProps());
  }

  private Properties baseProducerProps() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
    props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER,
        HeaderSchemaIdSerializer.class.getName());
    return props;
  }

  private void createTopics(String... topics) throws Exception {
    Properties adminProps = new Properties();
    adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    try (AdminClient admin = AdminClient.create(adminProps)) {
      admin.createTopics(
              Arrays.stream(topics)
                  .map(t -> new NewTopic(t, 1, (short) 1))
                  .collect(Collectors.toList()))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
  }

  private void waitForStoreToContainKeys(KafkaStreams streams, int expectedCount)
      throws Exception {
    long deadline = System.currentTimeMillis() + 30_000;
    while (System.currentTimeMillis() < deadline) {
      try {
        ReadOnlyKeyValueStore<SensorKey, ValueTimestampHeaders<SensorReading>> store =
            streams.store(StoreQueryParameters.fromNameAndType(
                STORE_NAME, new TimestampedKeyValueStoreWithHeadersType<>()));
        int count = 0;
        try (KeyValueIterator<SensorKey, ValueTimestampHeaders<SensorReading>> iter =
                 store.all()) {
          while (iter.hasNext()) {
            iter.next();
            count++;
          }
        }
        if (count >= expectedCount) {
          return;
        }
      } catch (Exception ignored) {
        // Store may not be ready yet.
      }
      Thread.sleep(200);
    }
    throw new AssertionError(
        "Store did not contain " + expectedCount + " entries within timeout");
  }

  private static void deleteRecursively(Path root) throws IOException {
    if (!Files.exists(root)) {
      return;
    }
    Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static class TimestampedKeyValueStoreWithHeadersType<K, V>
      implements QueryableStoreType<ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>>> {

    @Override
    public boolean accepts(StateStore stateStore) {
      return stateStore instanceof TimestampedKeyValueStoreWithHeaders
          && stateStore instanceof ReadOnlyKeyValueStore;
    }

    @Override
    public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> create(
        StateStoreProvider storeProvider, String storeName) {
      return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
    }
  }
}
