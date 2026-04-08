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

package io.confluent.kafka.streams.integration.dsl;

import static org.junit.jupiter.api.Assertions.*;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * DSL integration test for {@link TimestampedKeyValueStoreWithHeaders}.
 */
@Tag("IntegrationTest")
public class TimestampedKeyValueStoreWithHeadersDslIntegrationTest extends ClusterTestHarness {

    private static final String KEY_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"WordKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"word\",\"type\":\"string\"}"
            + "]"
            + "}";

    private static final String VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"TextLine\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"line\",\"type\":\"string\"}"
            + "]"
            + "}";

    private static final String AGG_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"WordCount\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"word\",\"type\":\"string\"},"
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    private static final String MAP_VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"MapWord\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration.dsl\","
            + "\"fields\":["
            + "  {\"name\":\"firstWord\",\"type\":\"string\"},"
            + "  {\"name\":\"count\",\"type\":\"long\"}"
            + "]"
            + "}";

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);
    private final Schema aggSchema = new Schema.Parser().parse(AGG_SCHEMA_JSON);
    private final Schema mapValueSchema = new Schema.Parser().parse(MAP_VALUE_SCHEMA_JSON);

    public TimestampedKeyValueStoreWithHeadersDslIntegrationTest() {
        super(1, true);
    }

    /**
     * Verifies `groupBy()` and `count()` work correctly using headers-aware stores.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGroupCountWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-count-supplier-input" + suffix;
        String outputTopic = "dsl-count-supplier-output" + suffix;
        String storeName = "dsl-count-supplier-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .flatMapValues(value ->
                Arrays.asList(value.get("line").toString().toLowerCase().split("\\W+")))
            .groupBy((key, word) -> {
                GenericRecord wordKey = new GenericData.Record(keySchema);
                wordKey.put("word", word);
                return wordKey;
            }, Grouped.with(keySerde, Serdes.String()))
            .count(Materialized.<GenericRecord, Long>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, Serdes.Long()));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-count-supplier-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("key1"), createTextLine("hello kafka streams"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("key2"), createTextLine("all streams lead to kafka"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("key3"), createTextLine("join kafka summit"))).get();
                producer.flush();
            }

            // Without caching: 3+5+3 = 11 records (one per word occurrence).
            // With caching: intermediate results are deduplicated, so we get
            // between 8 (unique words) and 11 records.
            int minExpected = 8;
            int maxExpected = 11;
            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeLongValueRecords(
                    outputTopic, "dsl-count-supplier-consumer" + suffix, maxExpected);

            assertTrue(results.size() >= minExpected && results.size() <= maxExpected,
                "Should have between " + minExpected + " and " + maxExpected
                    + " output records, got " + results.size());

            // Verify final counts from output records
            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, Long> record : results) {
                finalCounts.put(record.key().get("word").toString(), record.value());
            }
            assertEquals(3L, finalCounts.get("kafka"), "kafka should have count 3");
            assertEquals(2L, finalCounts.get("streams"), "streams should have count 2");
            assertEquals(1L, finalCounts.get("hello"), "hello should have count 1");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<Long>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(storeName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            // Verify key headers are preserved in the store.
            ValueTimestampHeaders<Long> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals(3L, kafkaResult.value(), "IQv1: kafka count should be 3");
            assertKeySchemaIdHeader(kafkaResult.headers(), "IQv1 get kafka");

            ValueTimestampHeaders<Long> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value(), "IQv1: streams count should be 2");
            assertKeySchemaIdHeader(streamsResult.headers(), "IQv1 get streams");

            ValueTimestampHeaders<Long> helloResult = store.get(createKey("hello"));
            assertNotNull(streamsResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value(), "IQv1: hello count should be 1");
            assertKeySchemaIdHeader(helloResult.headers(), "IQv1 get hello");

            // Send a null value to a KStream with flatMapValues causes
            // a NullPointerException, which crashes the streams instance into ERROR state.
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            assertEquals(KafkaStreams.State.ERROR, streams.state(),
                "Streams should be in ERROR state after null value hits flatMapValues");

            // Changelog verification (must run before streams crashes — use records already written)
            String changelogTopic = "dsl-count-supplier-test" + suffix + "-" + storeName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-count-changelog-consumer" + suffix, 8);
            assertTrue(changelogRecords.size() >= 8,
                "Changelog should have at least 8 records, got " + changelogRecords.size());

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() != null) {
                    assertKeySchemaIdHeader(record.headers(),
                        "changelog " + record.key().get("word"));
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `groupBy()` and `aggregate()` works correctly with headers-aware stores.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGroupAndAggregateWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-aggregate-input" + suffix;
        String outputTopic = "dsl-aggregate-output" + suffix;
        String storeName = "dsl-aggregate-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        
        GenericAvroSerde aggSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde))
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Null aggregation: returning null tombstones the key
                    if ("DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, aggSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-aggregate-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // Send 3 records for "kafka", 2 for "streams", 1 for "hello"
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("kafka"), createTextLine("first"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("kafka"), createTextLine("second"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("kafka"), createTextLine("third"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("streams"), createTextLine("first"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("streams"), createTextLine("second"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), createTextLine("first"))).get();
                producer.flush();
            }

            // Without caching: 6 records (one per input). With caching: 3 (deduplicated per key).
            int minExpected = 3;
            int maxExpected = 6;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-aggregate-consumer" + suffix, maxExpected);

            assertTrue(results.size() >= minExpected && results.size() <= maxExpected,
                "Should have between " + minExpected + " and " + maxExpected
                    + " output records, got " + results.size());

            // Verify final aggregated counts from output records
            Map<String, Long> finalCounts = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                finalCounts.put(
                    record.key().get("word").toString(),
                    (long) record.value().get("count"));
                assertSchemaIdHeaders(record.headers(), "aggregate output " + record.key());
            }
            assertEquals(3L, finalCounts.get("kafka"), "kafka should have count 3");
            assertEquals(2L, finalCounts.get("streams"), "streams should have count 2");
            assertEquals(1L, finalCounts.get("hello"), "hello should have count 1");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(storeName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> kafkaResult = store.get(createKey("kafka"));
            assertNotNull(kafkaResult, "IQv1: kafka should exist in store");
            assertEquals(3L, kafkaResult.value().get("count"), "IQv1: kafka count should be 3");
            assertSchemaIdHeaders(kafkaResult.headers(), "IQv1 get kafka");

            ValueTimestampHeaders<GenericRecord> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in store");
            assertEquals(2L, streamsResult.value().get("count"),
                "IQv1: streams count should be 2");
            assertSchemaIdHeaders(streamsResult.headers(), "IQv1 get streams");

            ValueTimestampHeaders<GenericRecord> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals(1L, helloResult.value().get("count"), "IQv1: hello count should be 1");
            assertSchemaIdHeaders(helloResult.headers(), "IQv1 get hello");

            // Add a record with value null for k1, which shouldn't work since "aggregate" operation skips null values.
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            ValueTimestampHeaders<GenericRecord> helloAfterTombstone = store.get(createKey("hello"));
            assertTrue(helloAfterTombstone != null && helloAfterTombstone.value() != null,
                "IQv1: hello shouldn't be tombstoned");
            assertEquals(1L, helloAfterTombstone.value().get("count"),
                "IQv1: hello count should still be 1");

            // Null aggregation: aggregator returns null for "DELETE" value, which tombstones the key
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), createTextLine("DELETE"))).get();
                producer.flush();
            }
            Thread.sleep(2000);

            // Re-fetch store reference in case of rebalance
            store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName,
                    new TimestampedKeyValueStoreWithHeadersType<>()));
            ValueTimestampHeaders<GenericRecord> helloDeleted = store.get(createKey("hello"));
            assertTrue(helloDeleted == null || helloDeleted.value() == null,
                "IQv1: hello should be tombstoned after aggregator returned null");
            // kafka and streams should still exist
            ValueTimestampHeaders<GenericRecord> kafkaStill = store.get(createKey("kafka"));
            assertNotNull(kafkaStill, "IQv1: kafka should still exist after hello null aggregation");
            assertEquals(3L, kafkaStill.value().get("count"), "IQv1: kafka count should still be 3");

            // Changelog verification
            String changelogTopic = "dsl-aggregate-test" + suffix + "-" + storeName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-aggregate-changelog-consumer" + suffix, 6);
            assertTrue(changelogRecords.size() >= 3,
                "Changelog should have at least 3 records, got " + changelogRecords.size());

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(),
                        "changelog " + record.key().get("word"));
                }
            }
        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `groupBy()` and `reduce()` works correctly with headers-aware stores.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldGroupAndReduceWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-reduce-input" + suffix;
        String outputTopic = "dsl-reduce-output" + suffix;
        String storeName = "dsl-reduce-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey(Grouped.with(keySerde, valueSerde))
            .reduce(
                (oldValue, newValue) -> {
                    // Null aggregation: returning null from reducer tombstones the key
                    if ("DELETE".equals(newValue.get("line").toString())) {
                        return null;
                    }
                    return newValue;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-reduce-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("k1"), createTextLine("first value"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("k1"), createTextLine("second value"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("k2"), createTextLine("only value"))).get();
                producer.flush();
            }

            // Without caching: 3 records (one per input record).
            // With caching: 2 records (k1 deduplicated to latest, k2 once).
            int minExpected = 2;
            int maxExpected = 3;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-reduce-consumer" + suffix, maxExpected);

            assertTrue(results.size() >= minExpected && results.size() <= maxExpected,
                "Should have between " + minExpected + " and " + maxExpected
                    + " output records, got " + results.size());

            // Verify final reduced values from output records
            Map<String, String> finalValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                finalValues.put(
                    record.key().get("word").toString(),
                    record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), "reduce output " + record.key());
            }
            assertEquals("second value", finalValues.get("k1"),
                "k1 should be reduced to the latest value");
            assertEquals("only value", finalValues.get("k2"),
                "k2 should have its only value");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(storeName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> k1Result = store.get(createKey("k1"));
            assertNotNull(k1Result, "IQv1: k1 should exist in store");
            assertEquals("second value", k1Result.value().get("line").toString(),
                "IQv1: k1 should have latest reduced value");
            assertSchemaIdHeaders(k1Result.headers(), "IQv1 get k1");

            ValueTimestampHeaders<GenericRecord> k2Result = store.get(createKey("k2"));
            assertNotNull(k2Result, "IQv1: k2 should exist in store");
            assertEquals("only value", k2Result.value().get("line").toString(),
                "IQv1: k2 should have its value");
            assertSchemaIdHeaders(k2Result.headers(), "IQv1 get k2");

            // Add a tombstone record for k1, which shouldn't work since "reduce" operation skips null values.
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("k1"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            ValueTimestampHeaders<GenericRecord> k1Tombstoned = store.get(createKey("k1"));
            assertTrue(k1Tombstoned != null && k1Tombstoned.value() != null, "IQv1: k1 shouldn't be tombstoned");
            ValueTimestampHeaders<GenericRecord> k2Still = store.get(createKey("k2"));
            assertNotNull(k2Still, "IQv1: k2 should still exist after k1 tombstone");
            assertEquals("only value", k2Still.value().get("line").toString());

            // Null aggregation: reducer returns null for "DELETE" value, which tombstones the key
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("k2"), createTextLine("DELETE"))).get();
                producer.flush();
            }
            Thread.sleep(2000);

            // Re-fetch store reference in case of rebalance
            store = streams.store(
                StoreQueryParameters.fromNameAndType(storeName,
                    new TimestampedKeyValueStoreWithHeadersType<>()));
            ValueTimestampHeaders<GenericRecord> k2Deleted = store.get(createKey("k2"));
            assertTrue(k2Deleted == null || k2Deleted.value() == null,
                "IQv1: k2 should be tombstoned after reducer returned null");
            // k1 should still be unaffected
            ValueTimestampHeaders<GenericRecord> k1StillExists = store.get(createKey("k1"));
            assertNotNull(k1StillExists, "IQv1: k1 should still exist after k2 null aggregation");
            assertEquals("second value", k1StillExists.value().get("line").toString());

            // Changelog verification
            String changelogTopic = "dsl-reduce-test" + suffix + "-" + storeName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-reduce-changelog-consumer" + suffix, 3);
            assertTrue(changelogRecords.size() >= 2,
                "Changelog should have at least 2 records (one per key), got "
                    + changelogRecords.size());

            // Verify non-tombstone changelog records have schema ID headers
            Map<String, byte[]> changelogValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                changelogValues.put(key, record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "changelog " + key);
                }
            }
            // Last changelog record for k1 should be a tombstone
            assertNotNull(changelogValues.get("k1"), "Changelog: k1 should have a value");
            assertNull(changelogValues.get("k2"), "Changelog: k2 shouldn't have a value");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `mapValues()` on a KTable works correctly with headers-aware stores.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMapValuesWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-mapvalues-input" + suffix;
        String outputTopic = "dsl-mapvalues-output" + suffix;
        String sourceStoreName = "dsl-mapvalues-source-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde mappedSerde = createValueSerde();

        // Source table is materialized with a headers-aware store.
        // mapValues is not materialized, so it uses a ValueGetter to read from the source store.
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(sourceStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .mapValues(value -> {
                GenericRecord mapped = new GenericData.Record(mapValueSchema);
                mapped.put("firstWord", value.get("line").toString().split("\\W+")[0]);
                mapped.put("count", (long) value.get("line").toString().length());
                return mapped;
            })
            .toStream()
            .to(outputTopic, Produced.with(keySerde, mappedSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-mapvalues-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), createTextLine("hello kafka streams"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("streams"), createTextLine("kafka streams"))).get();
                producer.flush();
            }

            int expected = 2;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-mapvalues-consumer" + suffix, expected);

            assertEquals(expected, results.size(),
                "Should have " + expected + " output records, got " + results.size());

            // Verify first mapped values: TextLine -> First word {word, firstWord=first word in line}
            Map<String, String> firstWordMappedValues = new HashMap<>();
            // Verify second mapped values: TextLine -> Line length {word, count=line length}
            Map<String, Long> lineLengthMappedValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String key = record.key().get("word").toString();
                firstWordMappedValues.put(key, record.value().get("firstWord").toString());
                lineLengthMappedValues.put(key, (long) record.value().get("count"));
                assertSchemaIdHeaders(record.headers(), "mapValues output " + record.key());
            }
            assertEquals("hello", firstWordMappedValues.get("hello"),
                "hello first word should be 'hello'");
            assertEquals("kafka", firstWordMappedValues.get("streams"),
                "streams first word should be 'kafka'");
            assertEquals(19L, lineLengthMappedValues.get("hello"),
                "hello line length should be 20");
            assertEquals(13L, lineLengthMappedValues.get("streams"),
                "streams line length should be 7");

            // IQv1 verification
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(sourceStoreName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Source store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in source store");
            assertEquals("hello kafka streams", helloResult.value().get("line").toString(),
                "IQv1: hello raw value");
            assertSchemaIdHeaders(helloResult.headers(), "IQv1 get hello");

            ValueTimestampHeaders<GenericRecord> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in source store");
            assertEquals("kafka streams", streamsResult.value().get("line").toString(),
                "IQv1: streams raw value");
            assertSchemaIdHeaders(streamsResult.headers(), "IQv1 get streams");

            // Tombstone: delete "hello" and verify it is removed from the store
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            ValueTimestampHeaders<GenericRecord> helloTombstoned = store.get(createKey("hello"));
            assertTrue(helloTombstoned == null || helloTombstoned.value() == null,
                "IQv1: hello should be tombstoned");
            ValueTimestampHeaders<GenericRecord> streamsStill = store.get(createKey("streams"));
            assertNotNull(streamsStill, "IQv1: streams should still exist after hello tombstone");
            assertEquals("kafka streams", streamsStill.value().get("line").toString());

            // Changelog verification for source store
            String changelogTopic = "dsl-mapvalues-test" + suffix + "-" + sourceStoreName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-mapvalues-changelog-consumer" + suffix, 3);
            assertTrue(changelogRecords.size() >= 2,
                "Changelog should have at least 2 records, got " + changelogRecords.size());

            Map<String, byte[]> changelogValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                changelogValues.put(key, record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "changelog " + key);
                }
            }
            assertNull(changelogValues.get("hello"), "Changelog: hello should end with tombstone");
            assertNotNull(changelogValues.get("streams"), "Changelog: streams should have a value");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verify `filter()` and `filterNot()` works correctly with a headers-aware store.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldFilterAndFilterNotWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-filter-input" + suffix;
        String outputTopic = "dsl-filter-output" + suffix;
        String filterStoreName = "dsl-filter-store" + suffix;
        String sourceStoreName = "dsl-filter-source-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        // The source table is materialized with a headers-aware store.
        // filterNot is not materialized, filter is materialized.
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(sourceStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .filterNot((key, value) -> value.get("line").toString().contains("kafka"))
            .filter((key, value) -> value.get("line").toString().length() > 10,
                Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(filterStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-filter-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("long"), createTextLine("this is a long long line"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("long2"), createTextLine("this is another long line"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("long3"), createTextLine("this is another long line with kafka"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("short"), createTextLine("line"))).get();
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-filter-consumer" + suffix, 2);

            assertEquals(2, results.size(), "Only the long lines without 'kafka' should pass filter");
            assertEquals("long", results.get(0).key().get("word").toString());
            assertEquals("this is a long long line", results.get(0).value().get("line").toString());
            assertSchemaIdHeaders(results.get(0).headers(), "filter output");
            assertEquals("long2", results.get(1).key().get("word").toString());
            assertEquals("this is another long line", results.get(1).value().get("line").toString());
            assertSchemaIdHeaders(results.get(1).headers(), "filter output2");

            // IQv1 verification — source store (all 4 records)
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> sourceStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    sourceStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(sourceStore, "Source store should be queryable");

            ValueTimestampHeaders<GenericRecord> srcLong = sourceStore.get(createKey("long"));
            assertNotNull(srcLong, "source store: 'long' should exist");
            assertEquals("this is a long long line", srcLong.value().get("line").toString());
            assertSchemaIdHeaders(srcLong.headers(), "source store: long");

            ValueTimestampHeaders<GenericRecord> srcLong2 = sourceStore.get(createKey("long2"));
            assertNotNull(srcLong2, "source store: 'long2' should exist");
            assertEquals("this is another long line", srcLong2.value().get("line").toString());
            assertSchemaIdHeaders(srcLong2.headers(), "source store: long2");

            ValueTimestampHeaders<GenericRecord> srcLong3 = sourceStore.get(createKey("long3"));
            assertNotNull(srcLong3, "source store: 'long3' should exist");
            assertEquals("this is another long line with kafka", srcLong3.value().get("line").toString());
            assertSchemaIdHeaders(srcLong3.headers(), "source store: long3");

            ValueTimestampHeaders<GenericRecord> srcShort = sourceStore.get(createKey("short"));
            assertNotNull(srcShort, "source store: 'short' should exist");
            assertEquals("line", srcShort.value().get("line").toString());
            assertSchemaIdHeaders(srcShort.headers(), "source store: short");

            // IQv1 verification — filter store (only long lines without 'kafka')
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> filterStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    filterStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(filterStore, "Filter store should be queryable");

            ValueTimestampHeaders<GenericRecord> longResult = filterStore.get(createKey("long"));
            assertNotNull(longResult, "filter store: 'long' should exist");
            assertSchemaIdHeaders(longResult.headers(), "filter store: long");
            ValueTimestampHeaders<GenericRecord> longResult2 = filterStore.get(createKey("long2"));
            assertNotNull(longResult2, "filter store: 'long2' should exist");
            assertSchemaIdHeaders(longResult2.headers(), "filter store: long2");

            ValueTimestampHeaders<GenericRecord> shortResult = filterStore.get(createKey("short"));
            assertTrue(shortResult == null || shortResult.value() == null,
                "filter store: 'short' should be filtered out");
            ValueTimestampHeaders<GenericRecord> long3Result = filterStore.get(createKey("long3"));
            assertTrue(long3Result == null || long3Result.value() == null,
                "filter store: 'long3' should be filtered out (contains 'kafka')");

            // Tombstone delete "long" and verify it is removed from the filter store
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("long"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            ValueTimestampHeaders<GenericRecord> longTombstoned = filterStore.get(createKey("long"));
            assertTrue(longTombstoned == null || longTombstoned.value() == null,
                "filter store: 'long' should be tombstoned");
            ValueTimestampHeaders<GenericRecord> long2Still = filterStore.get(createKey("long2"));
            assertNotNull(long2Still, "filter store: 'long2' should still exist");
            assertSchemaIdHeaders(long2Still.headers(), "filter store: long2 after tombstone");

            // Changelog verification for filter store
            String changelogTopic = "dsl-filter-test" + suffix + "-" + filterStoreName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-filter-changelog-consumer" + suffix, 3);
            assertTrue(changelogRecords.size() >= 2,
                "Changelog should have at least 2 records, got " + changelogRecords.size());

            Map<String, byte[]> changelogValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                changelogValues.put(key, record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "filter changelog " + key);
                }
            }
            assertNull(changelogValues.get("long"), "Changelog: long should end with tombstone");
            assertNotNull(changelogValues.get("long2"), "Changelog: long2 should have a value");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies KTable-KTable `join()` works correctly with headers-aware stores.
     * Left table contains full names, right table contains ages.
     * The join combines them into "FullName, age Age".
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldJoinTablesWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String namesTopic = "dsl-join-names" + suffix;
        String agesTopic = "dsl-join-ages" + suffix;
        String innerJoinOutputTopic = "dsl-inner-join-output" + suffix;
        String leftJoinOutputTopic = "dsl-left-join-output" + suffix;
        String outerJoinOutputTopic = "dsl-outer-join-output" + suffix;
        String innerJoinStoreName = "dsl-join-inner-store" + suffix;
        String leftJoinStoreName = "dsl-join-left-store" + suffix;
        String outerJoinStoreName = "dsl-join-outer-store" + suffix;

        createTopics(namesTopic, agesTopic, innerJoinOutputTopic, leftJoinOutputTopic, outerJoinOutputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KTable<GenericRecord, GenericRecord> namesTable =
            builder.table(namesTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders("names-store"))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));
        KTable<GenericRecord, GenericRecord> agesTable =
            builder.table(agesTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders("ages-store"))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        // inner join (returns null when age is "0" to test null join result)
        namesTable.join(agesTable,
                (name, age) -> {
                    if ("0".equals(age.get("line").toString())) {
                        return null;  // null join result tombstones the key
                    }
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    joined.put("line", name.get("line") + ", age " + age.get("line"));
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(innerJoinStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(innerJoinOutputTopic, Produced.with(keySerde, valueSerde));

        // left join
        namesTable.leftJoin(agesTable,
                (name, age) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String ageStr = age != null ? age.get("line").toString() : "unknown";
                    joined.put("line", name.get("line") + ", age " + ageStr);
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(leftJoinStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(leftJoinOutputTopic, Produced.with(keySerde, valueSerde));

        // outer join
        namesTable.outerJoin(agesTable,
                (name, age) -> {
                    GenericRecord joined = new GenericData.Record(valueSchema);
                    String nameStr = name != null ? name.get("line").toString() : "unknown";
                    String ageStr = age != null ? age.get("line").toString() : "unknown";
                    joined.put("line", nameStr + ", age " + ageStr);
                    return joined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                Stores.persistentTimestampedKeyValueStoreWithHeaders(outerJoinStoreName))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde))
                .toStream()
                .to(outerJoinOutputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-join-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                // Names table
                producer.send(new ProducerRecord<>(namesTopic,
                    createKey("alice"), createTextLine("Alice Smith"))).get();
                producer.send(new ProducerRecord<>(namesTopic,
                    createKey("bob"), createTextLine("Bob Jones"))).get();
                producer.send(new ProducerRecord<>(namesTopic,
                    createKey("carol"), createTextLine("Carol White"))).get();
                producer.flush();

                // Ages table
                producer.send(new ProducerRecord<>(agesTopic,
                    createKey("alice"), createTextLine("30"))).get();
                producer.send(new ProducerRecord<>(agesTopic,
                    createKey("bob"), createTextLine("25"))).get();
                producer.flush();
            }

            // Verify inner join
            List<ConsumerRecord<GenericRecord, GenericRecord>> innerJoinResults =
                consumeRecords(innerJoinOutputTopic, "dsl-inner-join-consumer" + suffix, 2);

            assertEquals(2, innerJoinResults.size(),
                "Should have 2 inner joined records");

            Map<String, String> innerJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : innerJoinResults) {
                String key = record.key().get("word").toString();
                innerJoinValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), "join output " + key);
            }
            assertEquals("Alice Smith, age 30", innerJoinValues.get("alice"));
            assertEquals("Bob Jones, age 25", innerJoinValues.get("bob"));
            assertNull(innerJoinValues.get("carol"));

            // IQv1 verification — names source store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> namesStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    "names-store", new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(namesStore, "Names store should be queryable");

            ValueTimestampHeaders<GenericRecord> aliceName = namesStore.get(createKey("alice"));
            assertNotNull(aliceName, "names store: alice should exist");
            assertEquals("Alice Smith", aliceName.value().get("line").toString());
            assertSchemaIdHeaders(aliceName.headers(), "names store: alice");

            ValueTimestampHeaders<GenericRecord> bobName = namesStore.get(createKey("bob"));
            assertNotNull(bobName, "names store: bob should exist");
            assertEquals("Bob Jones", bobName.value().get("line").toString());
            assertSchemaIdHeaders(bobName.headers(), "names store: bob");

            ValueTimestampHeaders<GenericRecord> carolName = namesStore.get(createKey("carol"));
            assertNotNull(carolName, "names store: carol should exist");
            assertEquals("Carol White", carolName.value().get("line").toString());
            assertSchemaIdHeaders(carolName.headers(), "names store: carol");

            // IQv1 verification — ages source store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> agesStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    "ages-store", new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(agesStore, "Ages store should be queryable");

            ValueTimestampHeaders<GenericRecord> aliceAge = agesStore.get(createKey("alice"));
            assertNotNull(aliceAge, "ages store: alice should exist");
            assertEquals("30", aliceAge.value().get("line").toString());
            assertSchemaIdHeaders(aliceAge.headers(), "ages store: alice");

            ValueTimestampHeaders<GenericRecord> bobAge = agesStore.get(createKey("bob"));
            assertNotNull(bobAge, "ages store: bob should exist");
            assertEquals("25", bobAge.value().get("line").toString());
            assertSchemaIdHeaders(bobAge.headers(), "ages store: bob");

            ValueTimestampHeaders<GenericRecord> carolAge = agesStore.get(createKey("carol"));
            assertNull(carolAge, "ages store: carol should not exist (no age provided)");

            // IQv1 verification — inner join store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(StoreQueryParameters.fromNameAndType(
                    innerJoinStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Inner join store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> aliceResult = store.get(createKey("alice"));
            assertNotNull(aliceResult, "IQv1 for inner join: alice should exist");
            assertEquals("Alice Smith, age 30",
                aliceResult.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), "IQv1 inner join get alice");

            ValueTimestampHeaders<GenericRecord> bobResult = store.get(createKey("bob"));
            assertNotNull(bobResult, "IQv1 for inner join: bob should exist");
            assertEquals("Bob Jones, age 25",
                bobResult.value().get("line").toString());
            assertSchemaIdHeaders(bobResult.headers(), "IQv1 inner join get bob");

            ValueTimestampHeaders<GenericRecord> carol = store.get(createKey("carol"));
            assertNull(carol, "IQv1 for inner join: carol should not exist");

            //Verify left join
            List<ConsumerRecord<GenericRecord, GenericRecord>> leftJoinResults =
                consumeRecords(leftJoinOutputTopic, "dsl-left-join-consumer" + suffix, 4);
            assertTrue(leftJoinResults.size() >= 3,
                "Should have at least 3 left joined records");
            Map<String, String> leftJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : leftJoinResults) {
                String key = record.key().get("word").toString();
                leftJoinValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), "left join output " + key);
            }
            assertEquals("Alice Smith, age 30", leftJoinValues.get("alice"));
            assertEquals("Bob Jones, age 25", leftJoinValues.get("bob"));
            assertEquals("Carol White, age unknown", leftJoinValues.get("carol"));

            // IQv1 verification for left join
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> leftJoinStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    leftJoinStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(leftJoinStore, "Left join store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> aliceResultLeft = leftJoinStore.get(createKey("alice"));
            assertNotNull(aliceResultLeft, "IQv1 for inner join: alice should exist");
            assertEquals("Alice Smith, age 30", aliceResultLeft.value().get("line").toString());
            assertSchemaIdHeaders(aliceResultLeft.headers(), "IQv1 inner join get alice");

            ValueTimestampHeaders<GenericRecord> bobResultLeft = leftJoinStore.get(createKey("bob"));
            assertNotNull(bobResultLeft, "IQv1 for left join: bob should exist");
            assertEquals("Bob Jones, age 25", bobResultLeft.value().get("line").toString());
            assertSchemaIdHeaders(bobResultLeft.headers(), "IQv1 left join get bob");

            ValueTimestampHeaders<GenericRecord> carolLeft = leftJoinStore.get(createKey("carol"));
            assertNotNull(carolLeft, "IQv1 for left join: carol should exist");
            assertEquals("Carol White, age unknown", carolLeft.value().get("line").toString());
            assertSchemaIdHeaders(carolLeft.headers(), "IQv1 left join get carol");

            // Verify outer join
            List<ConsumerRecord<GenericRecord, GenericRecord>> outerJoinResults =
                consumeRecords(outerJoinOutputTopic, "dsl-outer-join-consumer" + suffix, 4);
            assertTrue(outerJoinResults.size() >= 3, "Should have at least 3 outer joined records");
            Map<String, String> outerJoinValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : outerJoinResults) {
                String key = record.key().get("word").toString();
                outerJoinValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), "outer join output " + key);
            }
            assertEquals("Alice Smith, age 30", outerJoinValues.get("alice"));
            assertEquals("Bob Jones, age 25", outerJoinValues.get("bob"));
            assertEquals("Carol White, age unknown", outerJoinValues.get("carol"));

            // Verify IQv1 for outer join
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> outerJoinStore =
                streams.store(StoreQueryParameters.fromNameAndType(
                    outerJoinStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(outerJoinStore, "Outer join store should be accessible via IQv1");
            ValueTimestampHeaders<GenericRecord> aliceResultOuter = outerJoinStore.get(createKey("alice"));
            assertNotNull(aliceResultOuter, "IQv1 for outer join: alice should exist");
            assertEquals("Alice Smith, age 30", aliceResultOuter.value().get("line").toString());
            assertSchemaIdHeaders(aliceResultOuter.headers(), "IQv1 outer join get alice");
            ValueTimestampHeaders<GenericRecord> bobResultOuter = outerJoinStore.get(createKey("bob"));
            assertNotNull(bobResultOuter, "IQv1 for outer join: bob should exist");
            assertEquals("Bob Jones, age 25", bobResultOuter.value().get("line").toString());
            assertSchemaIdHeaders(bobResultOuter.headers(), "IQv1 outer join get bob");
            ValueTimestampHeaders<GenericRecord> carolOuter = outerJoinStore.get(createKey("carol"));
            assertNotNull(carolOuter, "IQv1 for outer join: carol should exist");
            assertEquals("Carol White, age unknown", carolOuter.value().get("line").toString());
            assertSchemaIdHeaders(carolOuter.headers(), "IQv1 outer join get carol");

            // Tombstone: delete alice's age and verify join stores update
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(agesTopic,
                    createKey("alice"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            // Inner join: alice should be removed (no age = no match)
            ValueTimestampHeaders<GenericRecord> aliceInnerTombstoned = store.get(createKey("alice"));
            assertTrue(aliceInnerTombstoned == null || aliceInnerTombstoned.value() == null,
                "IQv1 inner join: alice should be removed after age tombstone");
            ValueTimestampHeaders<GenericRecord> bobInnerStill = store.get(createKey("bob"));
            assertNotNull(bobInnerStill, "IQv1 inner join: bob should still exist");
            assertEquals("Bob Jones, age 25", bobInnerStill.value().get("line").toString());

            // Left join: alice should still exist but with "unknown" age
            ValueTimestampHeaders<GenericRecord> aliceLeftTombstoned = leftJoinStore.get(createKey("alice"));
            assertNotNull(aliceLeftTombstoned, "IQv1 left join: alice should still exist");
            assertEquals("Alice Smith, age unknown", aliceLeftTombstoned.value().get("line").toString(),
                "IQv1 left join: alice age should be unknown after tombstone");
            assertSchemaIdHeaders(aliceLeftTombstoned.headers(), "IQv1 left join tombstone get alice");

            // Outer join: alice should still exist but with "unknown" age
            ValueTimestampHeaders<GenericRecord> aliceOuterTombstoned = outerJoinStore.get(createKey("alice"));
            assertNotNull(aliceOuterTombstoned, "IQv1 outer join: alice should still exist");
            assertEquals("Alice Smith, age unknown", aliceOuterTombstoned.value().get("line").toString(),
                "IQv1 outer join: alice age should be unknown after tombstone");
            assertSchemaIdHeaders(aliceOuterTombstoned.headers(), "IQv1 outer join tombstone get alice");

            // Null join: send age "0" for bob, which makes the inner joiner return null
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(agesTopic,
                    createKey("bob"), createTextLine("0"))).get();
                producer.flush();
            }
            Thread.sleep(2000);

            // Re-fetch store references in case of rebalance
            store = streams.store(StoreQueryParameters.fromNameAndType(
                innerJoinStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            leftJoinStore = streams.store(StoreQueryParameters.fromNameAndType(
                leftJoinStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));
            outerJoinStore = streams.store(StoreQueryParameters.fromNameAndType(
                outerJoinStoreName, new TimestampedKeyValueStoreWithHeadersType<>()));

            // Inner join: bob should be tombstoned because joiner returned null for age "0"
            ValueTimestampHeaders<GenericRecord> bobInnerNullJoin = store.get(createKey("bob"));
            assertTrue(bobInnerNullJoin == null || bobInnerNullJoin.value() == null,
                "IQv1 inner join: bob should be tombstoned after null join result");

            // Left join: bob should still exist (left joiner doesn't return null for age "0")
            ValueTimestampHeaders<GenericRecord> bobLeftNullJoin = leftJoinStore.get(createKey("bob"));
            assertNotNull(bobLeftNullJoin, "IQv1 left join: bob should still exist after null inner join");
            assertEquals("Bob Jones, age 0", bobLeftNullJoin.value().get("line").toString(),
                "IQv1 left join: bob should have age 0");

            // Outer join: bob should still exist
            ValueTimestampHeaders<GenericRecord> bobOuterNullJoin = outerJoinStore.get(createKey("bob"));
            assertNotNull(bobOuterNullJoin, "IQv1 outer join: bob should still exist after null inner join");
            assertEquals("Bob Jones, age 0", bobOuterNullJoin.value().get("line").toString(),
                "IQv1 outer join: bob should have age 0");

            // Changelog verification for inner join store
            String innerChangelogTopic = "dsl-join-test" + suffix + "-" + innerJoinStoreName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> innerChangelogRecords =
                consumeChangelogRecords(innerChangelogTopic, "dsl-join-inner-changelog-consumer" + suffix, 3);
            assertTrue(innerChangelogRecords.size() >= 2,
                "Inner join changelog should have at least 2 records, got " + innerChangelogRecords.size());

            Map<String, byte[]> innerChangelogValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, byte[]> record : innerChangelogRecords) {
                String key = record.key().get("word").toString();
                innerChangelogValues.put(key, record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "inner join changelog " + key);
                }
            }
            // alice should be tombstoned in inner join changelog after age deletion
            assertNull(innerChangelogValues.get("alice"),
                "Inner join changelog: alice should end with tombstone");
            // bob should be tombstoned in inner join changelog after null join result
            assertNull(innerChangelogValues.get("bob"),
                "Inner join changelog: bob should end with tombstone after null join");

            // Changelog verification for left join store
            String leftChangelogTopic = "dsl-join-test" + suffix + "-" + leftJoinStoreName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> leftChangelogRecords =
                consumeChangelogRecords(leftChangelogTopic, "dsl-join-left-changelog-consumer" + suffix, 4);
            assertTrue(leftChangelogRecords.size() >= 3,
                "Left join changelog should have at least 3 records, got " + leftChangelogRecords.size());

            // Verify non-tombstone records have headers
            for (ConsumerRecord<GenericRecord, byte[]> record : leftChangelogRecords) {
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "left join changelog " + record.key().get("word"));
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that headers survive through toStream() and merge() operations.
     * Two KTables are materialized with headers-aware stores, converted to streams,
     * merged, and the output is checked for schema ID headers.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldMergeStreamsWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic1 = "dsl-merge-input1" + suffix;
        String inputTopic2 = "dsl-merge-input2" + suffix;
        String outputTopic = "dsl-merge-output" + suffix;
        String storeName1 = "dsl-merge-store1" + suffix;
        String storeName2 = "dsl-merge-store2" + suffix;

        createTopics(inputTopic1, inputTopic2, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        KTable<GenericRecord, GenericRecord> table1 = builder.table(inputTopic1,
            Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName1))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        KTable<GenericRecord, GenericRecord> table2 = builder.table(inputTopic2,
            Consumed.with(keySerde, valueSerde),
            Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName2))
                .withKeySerde(keySerde)
                .withValueSerde(valueSerde));

        table1.toStream()
            .merge(table2.toStream())
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-merge-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic1,
                    createKey("alice"), createTextLine("alice table 1"))).get();
                producer.send(new ProducerRecord<>(inputTopic1,
                    createKey("bob"), createTextLine("bob table 1"))).get();
                producer.send(new ProducerRecord<>(inputTopic2,
                    createKey("bob"), createTextLine("bob table 2"))).get();
                producer.send(new ProducerRecord<>(inputTopic2,
                    createKey("carol"), createTextLine("carol table 2"))).get();
                producer.send(new ProducerRecord<>(inputTopic2,
                    createKey("dave"), createTextLine("dave table 2"))).get();
                producer.send(new ProducerRecord<>(inputTopic1,
                    createKey("alice"), createTextLine("alice table 1 again"))).get();
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-merge-consumer" + suffix, 6);

            assertEquals(6, results.size(), "Should have 5 merged records");

            Map<String, String> mergedValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String key = record.key().get("word").toString();
                mergedValues.put(key, record.value().get("line").toString());
                assertSchemaIdHeaders(record.headers(), "merge output " + key);
            }
            assertEquals("alice table 1 again", mergedValues.get("alice"));
            assertEquals("bob table 2", mergedValues.get("bob"));
            assertEquals("carol table 2", mergedValues.get("carol"));
            assertEquals("dave table 2", mergedValues.get("dave"));

            // IQv1 verification for store 1
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store1 =
                streams.store(StoreQueryParameters.fromNameAndType(
                    storeName1, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store1, "Store1 should be accessible via IQv1");

            // verify store 1
            ValueTimestampHeaders<GenericRecord> aliceResult = store1.get(createKey("alice"));
            assertNotNull(aliceResult, "IQv1: alice should exist in store1");
            assertEquals("alice table 1 again", aliceResult.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), "IQv1 store1 get alice");

            ValueTimestampHeaders<GenericRecord> bobResult1 = store1.get(createKey("bob"));
            assertNotNull(aliceResult, "IQv1: bob should exist in store1");
            assertEquals("bob table 1", bobResult1.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), "IQv1 store1 get bob");

            // IQv1 verification for store 2
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store2 =
                streams.store(StoreQueryParameters.fromNameAndType(
                    storeName2, new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store2, "Store2 should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> bobResult2 = store2.get(createKey("bob"));
            assertNotNull(aliceResult, "IQv1: bob should exist in store1");
            assertEquals("bob table 2", bobResult2.value().get("line").toString());
            assertSchemaIdHeaders(aliceResult.headers(), "IQv1 store1 get bob");

            ValueTimestampHeaders<GenericRecord> carolResult = store2.get(createKey("carol"));
            assertNotNull(carolResult, "IQv1: carol should exist in store2");
            assertEquals("carol table 2", carolResult.value().get("line").toString());
            assertSchemaIdHeaders(carolResult.headers(), "IQv1 store2 get carol");

            ValueTimestampHeaders<GenericRecord> daveResult = store2.get(createKey("dave"));
            assertNotNull(daveResult, "IQv1: dave should exist in store2");
            assertEquals("dave table 2", daveResult.value().get("line").toString());
            assertSchemaIdHeaders(carolResult.headers(), "IQv1 store2 get carol");

            // Tombstone: delete alice from table1 and verify it is removed from store1
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic1,
                    createKey("alice"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            ValueTimestampHeaders<GenericRecord> aliceTombstoned = store1.get(createKey("alice"));
            assertTrue(aliceTombstoned == null || aliceTombstoned.value() == null,
                "IQv1: alice should be tombstoned from store1");
            ValueTimestampHeaders<GenericRecord> bobStill = store1.get(createKey("bob"));
            assertNotNull(bobStill, "IQv1: bob should still exist in store1");
            assertEquals("bob table 1", bobStill.value().get("line").toString());

            // Changelog verification for store1
            String changelogTopic1 = "dsl-merge-test" + suffix + "-" + storeName1 + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic1, "dsl-merge-changelog-consumer" + suffix, 3);
            assertTrue(changelogRecords.size() >= 2,
                "Store1 changelog should have at least 2 records, got " + changelogRecords.size());

            Map<String, byte[]> changelogValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                changelogValues.put(key, record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "merge changelog " + key);
                }
            }
            assertNull(changelogValues.get("alice"), "Changelog: alice should end with tombstone");
            assertNotNull(changelogValues.get("bob"), "Changelog: bob should have a value");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies `transformValues()` on a KTable works correctly with headers-aware stores,
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldTransformValuesWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-transform-input" + suffix;
        String outputTopic = "dsl-transform-output" + suffix;
        String sourceStoreName = "dsl-transform-source-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde mappedSerde = createValueSerde();

        // Source table is materialized with a headers-aware store.
        // transformValues is not materialized.
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(sourceStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .transformValues(() -> new ValueTransformerWithKey<GenericRecord, GenericRecord, GenericRecord>() {
                @Override
                public void init(ProcessorContext context) {}

                @Override
                public GenericRecord transform(GenericRecord key, GenericRecord value) {
                    if (value == null) {
                        return null;
                    }
                    GenericRecord mapped = new GenericData.Record(mapValueSchema);
                    mapped.put("firstWord", value.get("line").toString().split("\\W+")[0]);
                    mapped.put("count", (long) value.get("line").toString().length());
                    return mapped;
                }

                @Override
                public void close() {}
            })
            .toStream()
            .to(outputTopic, Produced.with(keySerde, mappedSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-transform-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), createTextLine("hello kafka streams"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("streams"), createTextLine("kafka streams"))).get();
                producer.flush();
            }

            int expected = 2;
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-transform-consumer" + suffix, expected);

            assertEquals(expected, results.size(),
                "Should have " + expected + " output records, got " + results.size());

            // Verify transformed values
            Map<String, String> firstWords = new HashMap<>();
            Map<String, Long> lengths = new HashMap<>();
            for (ConsumerRecord<GenericRecord, GenericRecord> record : results) {
                String key = record.key().get("word").toString();
                firstWords.put(key, record.value().get("firstWord").toString());
                lengths.put(key, (long) record.value().get("count"));
                assertSchemaIdHeaders(record.headers(), "transformValues output " + key);
            }
            assertEquals("hello", firstWords.get("hello"), "hello first word");
            assertEquals("kafka", firstWords.get("streams"), "streams first word");
            assertEquals(19L, lengths.get("hello"), "hello line length");
            assertEquals(13L, lengths.get("streams"), "streams line length");

            // IQv1 verification — query the source store (raw TextLine values)
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(sourceStoreName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Source store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in source store");
            assertEquals("hello kafka streams", helloResult.value().get("line").toString());
            assertSchemaIdHeaders(helloResult.headers(), "IQv1 get hello");

            ValueTimestampHeaders<GenericRecord> streamsResult = store.get(createKey("streams"));
            assertNotNull(streamsResult, "IQv1: streams should exist in source store");
            assertEquals("kafka streams", streamsResult.value().get("line").toString());
            assertSchemaIdHeaders(streamsResult.headers(), "IQv1 get streams");

            // Tombstone: delete "hello" and verify it is removed from the source store
            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("hello"), (GenericRecord) null)).get();
                producer.flush();
            }
            Thread.sleep(2000);

            // Re-fetch store reference in case of rebalance
            store = streams.store(
                StoreQueryParameters.fromNameAndType(sourceStoreName,
                    new TimestampedKeyValueStoreWithHeadersType<>()));

            ValueTimestampHeaders<GenericRecord> helloTombstoned = store.get(createKey("hello"));
            assertTrue(helloTombstoned == null || helloTombstoned.value() == null,
                "IQv1: hello should be tombstoned");
            ValueTimestampHeaders<GenericRecord> streamsStill = store.get(createKey("streams"));
            assertNotNull(streamsStill, "IQv1: streams should still exist");
            assertEquals("kafka streams", streamsStill.value().get("line").toString());

            // Changelog verification for source store
            String changelogTopic = "dsl-transform-test" + suffix + "-" + sourceStoreName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-transform-changelog-consumer" + suffix, 3);
            assertTrue(changelogRecords.size() >= 2,
                "Changelog should have at least 2 records, got " + changelogRecords.size());

            Map<String, byte[]> changelogValues = new HashMap<>();
            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                String key = record.key().get("word").toString();
                changelogValues.put(key, record.value());
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(), "transform changelog " + key);
                }
            }
            assertNull(changelogValues.get("hello"), "Changelog: hello should end with tombstone");
            assertNotNull(changelogValues.get("streams"), "Changelog: streams should have a value");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Verifies that {@code prefixScan()} works correctly on a headers-aware store
     * with Schema Registry header-based serialization.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void shouldPrefixScanWithHeaders(boolean cachingEnabled) throws Exception {
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        String inputTopic = "dsl-prefixscan-input" + suffix;
        String outputTopic = "dsl-prefixscan-output" + suffix;
        String storeName = "dsl-prefixscan-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        // Materialize a KTable with a headers-aware store, then stream it out.
        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(
                builder.build(), "dsl-prefixscan-test" + suffix, cachingEnabled);

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("ka"), createTextLine("value ka"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("kb"), createTextLine("value kb"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("zz"), createTextLine("value zz"))).get();
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-prefixscan-consumer" + suffix, 3);
            assertEquals(3, results.size(), "Should have 3 output records");

            // IQv1: get the store
            ReadOnlyKeyValueStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(storeName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            // prefixScan with "ka" — verifies SR serialization works for prefix key
            // and headers are correctly deserialized on the return path.
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> kaResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("ka"), keySerde.serializer())) {
                while (iter.hasNext()) {
                    kaResults.add(iter.next());
                }
            }

            assertEquals(1, kaResults.size(), "prefixScan('ka') should return 1 entry");
            assertEquals("ka", kaResults.get(0).key.get("word").toString());
            assertEquals("value ka", kaResults.get(0).value.value().get("line").toString());
            assertSchemaIdHeaders(kaResults.get(0).value.headers(), "prefixScan ka");

            // prefixScan with "kb"
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> kbResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("kb"), keySerde.serializer())) {
                while (iter.hasNext()) {
                    kbResults.add(iter.next());
                }
            }

            assertEquals(1, kbResults.size(), "prefixScan('kb') should return 1 entry");
            assertEquals("kb", kbResults.get(0).key.get("word").toString());
            assertEquals("value kb", kbResults.get(0).value.value().get("line").toString());
            assertSchemaIdHeaders(kbResults.get(0).value.headers(), "prefixScan kb");

            // prefixScan with "zz"
            List<KeyValue<GenericRecord, ValueTimestampHeaders<GenericRecord>>> zzResults =
                new ArrayList<>();
            try (KeyValueIterator<GenericRecord, ValueTimestampHeaders<GenericRecord>> iter =
                     store.prefixScan(createKey("zz"), keySerde.serializer())) {
                while (iter.hasNext()) {
                    zzResults.add(iter.next());
                }
            }
            assertEquals(1, zzResults.size(), "prefixScan('zz') should return 1 entry");
            assertEquals("zz", zzResults.get(0).key.get("word").toString());
            assertEquals("value zz", zzResults.get(0).value.value().get("line").toString());
            assertSchemaIdHeaders(zzResults.get(0).value.headers(), "prefixScan zz");

            // IQv1 verification
            ValueTimestampHeaders<GenericRecord> kaGet = store.get(createKey("ka"));
            assertNotNull(kaGet, "IQv1: ka should exist in store");
            assertEquals("value ka", kaGet.value().get("line").toString());
            assertSchemaIdHeaders(kaGet.headers(), "IQv1 get ka");

            ValueTimestampHeaders<GenericRecord> kbGet = store.get(createKey("kb"));
            assertNotNull(kbGet, "IQv1: kb should exist in store");
            assertEquals("value kb", kbGet.value().get("line").toString());
            assertSchemaIdHeaders(kbGet.headers(), "IQv1 get kb");

            ValueTimestampHeaders<GenericRecord> zzGet = store.get(createKey("zz"));
            assertNotNull(zzGet, "IQv1: zz should exist in store");
            assertEquals("value zz", zzGet.value().get("line").toString());
            assertSchemaIdHeaders(zzGet.headers(), "IQv1 get zz");

            // Changelog verification
            String changelogTopic = "dsl-prefixscan-test" + suffix + "-" + storeName + "-changelog";
            List<ConsumerRecord<GenericRecord, byte[]>> changelogRecords =
                consumeChangelogRecords(changelogTopic, "dsl-prefixscan-changelog-consumer" + suffix, 3);
            assertTrue(changelogRecords.size() >= 3,
                "Changelog should have at least 3 records, got " + changelogRecords.size());

            for (ConsumerRecord<GenericRecord, byte[]> record : changelogRecords) {
                if (record.value() != null) {
                    assertSchemaIdHeaders(record.headers(),
                        "changelog " + record.key().get("word"));
                }
            }

        } finally {
            closeStreams(streams);
        }
    }

    private void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            List<NewTopic> topics = Arrays.stream(topicNames)
                .map(name -> new NewTopic(name, 1, (short) 1))
                .collect(Collectors.toList());
            admin.createTopics(topics).all().get(30, TimeUnit.SECONDS);
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

    private Properties createStreamsProps(String appId, boolean cachingEnabled) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
//        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        if (cachingEnabled) {
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        } else {
            props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
            props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
        }
        return props;
    }

    private Properties createProducerProps() {
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

    private Properties createConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return props;
    }

    private Properties createLongValueConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.LongDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return props;
    }

    private KafkaStreams startStreamsAndAwaitRunning(
        org.apache.kafka.streams.Topology topology,
        String appId,
        boolean cachingEnabled) throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(topology, createStreamsProps(appId, cachingEnabled));
        streams.cleanUp();
        streams.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) {
                startedLatch.countDown();
            }
        });
        streams.start();
        assertTrue(startedLatch.await(30, TimeUnit.SECONDS), "KafkaStreams should reach RUNNING state");
        return streams;
    }

    private void closeStreams(KafkaStreams streams) {
        if (streams != null) {
            streams.close(Duration.ofSeconds(10));
        }
    }

    private List<ConsumerRecord<GenericRecord, Long>> consumeLongValueRecords(
        String topic, String groupId, int expectedCount) {
        List<ConsumerRecord<GenericRecord, Long>> results = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, Long> consumer =
                 new KafkaConsumer<>(createLongValueConsumerProps(groupId))) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<GenericRecord, Long> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, Long> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    private void assertSchemaIdHeaders(Headers headers, String context) {
        Header keySchemaIdHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keySchemaIdHeader, context + ": should have __key_schema_id header");
        byte[] keyHeaderBytes = keySchemaIdHeader.value();
        assertEquals(17, keyHeaderBytes.length, context + ": Key GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, keyHeaderBytes[0], context + ": Key header should have V1 magic byte");

        Header valueSchemaIdHeader = headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
        assertNotNull(valueSchemaIdHeader, context + ": should have __value_schema_id header");
        byte[] valueHeaderBytes = valueSchemaIdHeader.value();
        assertEquals(17, valueHeaderBytes.length, context + ": Value GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, valueHeaderBytes[0], context + ": Value header should have V1 magic byte");
    }


    private void assertKeySchemaIdHeader(Headers headers, String context) {
        Header keySchemaIdHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(keySchemaIdHeader, context + ": should have __key_schema_id header");
        byte[] keyHeaderBytes = keySchemaIdHeader.value();
        assertEquals(17, keyHeaderBytes.length, context + ": Key GUID header should be 17 bytes");
        assertEquals(SchemaId.MAGIC_BYTE_V1, keyHeaderBytes[0], context + ": Key header should have V1 magic byte");
    }

    private GenericRecord createKey(String word) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("word", word);
        return key;
    }

    private GenericRecord createTextLine(String line) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("line", line);
        return value;
    }

    private List<ConsumerRecord<GenericRecord, GenericRecord>> consumeRecords(
        String topic, String groupId, int expectedCount) {
        List<ConsumerRecord<GenericRecord, GenericRecord>> results = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                 new KafkaConsumer<>(createConsumerProps(groupId))) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    /**
     * Consumes records from a changelog topic using byte[] value deserializer
     * (since changelog values are in internal ValueAndTimestamp format).
     * Returns records so callers can verify headers and tombstones.
     */
    private List<ConsumerRecord<GenericRecord, byte[]>> consumeChangelogRecords(
        String changelogTopic, String groupId, int expectedCount) {
        List<ConsumerRecord<GenericRecord, byte[]>> results = new ArrayList<>();
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        try (KafkaConsumer<GenericRecord, byte[]> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(changelogTopic));
            long deadline = System.currentTimeMillis() + 30_000;
            while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<GenericRecord, byte[]> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<GenericRecord, byte[]> record : records) {
                    results.add(record);
                }
            }
        }
        return results;
    }

    /**
     * Custom QueryableStoreType for querying TimestampedKeyValueStoreWithHeaders directly
     * without facade wrapping. This returns the full ValueTimestampHeaders wrapper.
     */
    private static class TimestampedKeyValueStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>>> {

        @Override
        public boolean accepts(final StateStore stateStore) {
            return stateStore instanceof TimestampedKeyValueStoreWithHeaders
                && stateStore instanceof ReadOnlyKeyValueStore;
        }

        @Override
        public ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> create(
            final StateStoreProvider storeProvider,
            final String storeName) {
            return new CompositeReadOnlyKeyValueStore<>(storeProvider, this, storeName);
        }
    }
}
