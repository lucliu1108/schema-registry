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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.ClusterTestHarness;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.HeadersBytesStoreSupplier;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyWindowStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import java.util.stream.Stream;

@Tag("IntegrationTest")
public class TimestampedWindowStoreWithHeadersDslIntegrationTest extends ClusterTestHarness {

    private static final String KEY_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"WordKey\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"}]}";
    private static final String VALUE_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"TextLine\",\"fields\":[{\"name\":\"line\",\"type\":\"string\"}]}";
    private static final String AGG_SCHEMA_JSON = "{\"type\":\"record\",\"name\":\"WordCount\",\"fields\":[{\"name\":\"word\",\"type\":\"string\"},{\"name\":\"count\",\"type\":\"long\"}]}";
    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);
    private final Schema aggSchema = new Schema.Parser().parse(AGG_SCHEMA_JSON);

    public TimestampedWindowStoreWithHeadersDslIntegrationTest() {
        super(1, true);
    }

    // Parameter provider for cache + grace combinations
    private static Stream<Arguments> cacheAndGraceParams() {
        return Stream.of(
            Arguments.of(true, true),    // cached + grace
            Arguments.of(true, false),   // cached + no grace
            Arguments.of(false, true),   // uncached + grace
            Arguments.of(false, false)   // uncached + no grace
        );
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCountWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "window-input" + suffix;
        String storeName = "window-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows windows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(windows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "window-test" + suffix, cachingEnabled);

        long baseTime = 1000000L;
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("hello world from kafka"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 1000), createKey("kafka"), createTextLine("processing streams in real time"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        // 1. Verify IQv1 Fetch - initial count
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        long windowStart = (baseTime / windowSize.toMillis()) * windowSize.toMillis();
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
            assertTrue(it.hasNext(), "Should find windowed result");
            KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
            assertEquals(2L, next.value.value(), "Initial count should be 2");
            assertKeySchemaIdHeader(next.value.headers(), "IQv1 Window Header");
        }

        // 2. Test null value handling - null inputs shouldn't be skipped
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send null value for "kafka" key - should be skipped, not counted
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 3000), createKey("kafka"), (GenericRecord) null)).get();
            // Send normal value for "streams" key
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 4000), createKey("streams"), createTextLine("streams value"))).get();
            producer.flush();
        }

        Thread.sleep(2000);

        // Re-fetch store
        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        // Verify "kafka" count is 3
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
            assertTrue(it.hasNext(), "kafka window should still exist after null value");
            KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
            assertEquals(3L, next.value.value(), "kafka count should still be 3 (null was not skipped)");
            assertNotNull(next.value.value(), "kafka count should not be null");
            assertKeySchemaIdHeader(next.value.headers(), "IQv1 after null input");
        }

        // Verify "streams" has count of 1
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
            assertTrue(it.hasNext(), "streams window should exist");
            assertEquals(1L, it.next().value.value(), "streams count should be 1");
        }

        // 3. Test grace period if enabled
        if (graceEnabled) {
            try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
                // Advance stream time past window end but within grace
                // Window: [1000000, 1010000), ends at 1010000, grace until 1015000
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 12000),
                    createKey("other"), createTextLine("time advance message"))).get();
                // Now send late record (timestamp in original window, but stream time already advanced)
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 5000),
                    createKey("kafka"), createTextLine("within grace period"))).get();
                producer.flush();
            }

            Thread.sleep(3000);

            store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

            // Verify late record was counted (count should be 4: 3 original + 1 late)
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
                assertTrue(it.hasNext(), "Should still find windowed result");
                KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
                assertEquals(4L, next.value.value(), "Count should be 4 after late arrival within grace");
                assertKeySchemaIdHeader(next.value.headers(), "IQv1 after grace period");
            }

            // Send too-late record beyond grace period
            try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
                // Advance stream time beyond window + grace (past 1015000)
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 20000),
                    createKey("other2"), createTextLine("expire grace now"))).get();
                // Now send too-late record (stream time is past window + grace)
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 8000),
                    createKey("kafka"), createTextLine("too late rejected"))).get();
                producer.flush();
            }

            Thread.sleep(3000);

            // Re-fetch store
            store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

            // Verify too-late record was dropped (count still 4)
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(windowStart), Instant.ofEpochMilli(windowStart + windowSize.toMillis()))) {
                assertTrue(it.hasNext(), "Should still find windowed result");
                KeyValue<Long, ValueTimestampHeaders<Long>> next = it.next();
                assertEquals(4L, next.value.value(), "Count should still be 4, too-late record dropped");
            }
        }

        // Close streams first to force cache flush
        closeStreams(streams);

        // 3. Verify Changelog Headers
        String changelog = "window-test" + suffix + "-" + storeName + "-changelog";
        // Expected changelog count varies:
        // cached, no grace: 2 (deduplicated final state)
        // uncached, no grace: 3 (individual updates)
        // cached, with grace: 3 (original window + late update)
        // uncached, with grace: 5+ (initial updates + late update)
        int expectedMinRecords = cachingEnabled ? (graceEnabled ? 3 : 2) : (graceEnabled ? 4 : 3);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "window-cg-" + testId, expectedMinRecords + 2);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Changelog header for windowed key");
            }
        }

    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldAggregateWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "window-agg-input" + suffix;
        String storeName = "window-agg-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
                    // Null aggregation: returning null tombstones the window
                    if ("DELETE".equals(value.get("line").toString())) {
                        return null;
                    }
                    GenericRecord updated = new GenericData.Record(aggSchema);
                    updated.put("word", key.get("word").toString());
                    updated.put("count", (long) agg.get("count") + 1L);
                    return updated;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(aggSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "window-agg-test" + suffix, cachingEnabled);

        long baseTime = 2000000L;
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send 3 records for "kafka" in same window
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("hello world from kafka"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 1000), createKey("kafka"), createTextLine("processing streams in real time"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 2000), createKey("kafka"), createTextLine("headers are preserved"))).get();
            // Send 2 records for "streams" in same window
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 3000), createKey("streams"), createTextLine("hello world from kafka"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 4000), createKey("streams"), createTextLine("processing streams in real time"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        // Verify IQv1
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        // Verify IQv1 Fetch for "kafka"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed aggregate for kafka");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(3L, next.value.value().get("count"), "kafka count should be 3");
            assertEquals("kafka", next.value.value().get("word").toString());
            assertSchemaIdHeaders(next.value.headers(), "IQv1 kafka aggregate");
        }

        // Verify IQv1 Fetch for "streams"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed aggregate for streams");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(2L, next.value.value().get("count"), "streams count should be 2");
            assertEquals("streams", next.value.value().get("word").toString());
            assertSchemaIdHeaders(next.value.headers(), "IQv1 streams aggregate");
        }

        // Null aggregation: send DELETE value to tombstone the window
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 10000), createKey("hello"), createTextLine("hello world from kafka"))).get();
            producer.flush();
        }
        Thread.sleep(2000);

        // Verify "hello" exists
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("hello"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "hello should exist before DELETE");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            assertEquals(1L, next.value.value().get("count"));
            assertSchemaIdHeaders(next.value.headers(), "IQv1 hello aggregate");
        }

        // Send DELETE to tombstone "hello" window
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 11000), createKey("hello"), createTextLine("DELETE"))).get();
            producer.flush();
        }
        Thread.sleep(2000);

        // Re-fetch store reference in case of rebalance
        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        // Verify "hello" is tombstoned
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("hello"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            if (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
                assertNull(next.value.value(), "hello should be tombstoned after DELETE");
            }
        }

        // Verify "kafka" and "streams" still exist
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "kafka should still exist after hello DELETE");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> kafkaResult = it.next();
            assertEquals(3L, kafkaResult.value.value().get("count"));
            assertSchemaIdHeaders(kafkaResult.value.headers(), "IQv1 kafka aggregate");
        }

        closeStreams(streams);
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldCountWithHoppingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "window-hop-input" + suffix;
        String storeName = "window-hop-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration advanceBy = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(advanceBy)
            : TimeWindows.ofSizeWithNoGrace(windowSize).advanceBy(advanceBy);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde)
                .withValueSerde(Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "window-hop-test" + suffix, cachingEnabled);

        long baseTime = 3000000L;
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send records at specific times to create overlapping windows for "kafka"
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("quick brown fox jumps"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 3000), createKey("kafka"), createTextLine("windowed by time"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 7000), createKey("kafka"), createTextLine("late events within grace"))).get();

            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 2000), createKey("streams"), createTextLine("quick brown fox jumps"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 6000), createKey("streams"), createTextLine("windowed by time"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        // Verify multiple overlapping windows exist for "kafka"
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        List<KeyValue<Long, ValueTimestampHeaders<Long>>> windows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                windows.add(window);
                assertKeySchemaIdHeader(window.value.headers(), "IQv1 hopping window " + window.key);
            }
        }

        // With hopping windows (10s size, 5s advance), we should have 3 overlapping windows
        assertTrue(windows.size() >= 3, "Should have at least 3 overlapping windows for kafka, got " + windows.size());

        // Verify each window has correct count based on which records fall within it
        for (KeyValue<Long, ValueTimestampHeaders<Long>> window : windows) {
            long count = window.value.value();
            assertTrue(count >= 1 && count <= 3, "kafka window count should be between 1 and 3, got " + count);
        }

        // Verify "streams" key also has overlapping windows
        List<KeyValue<Long, ValueTimestampHeaders<Long>>> streamsWindows = new ArrayList<>();
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            while (it.hasNext()) {
                KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                streamsWindows.add(window);
                assertKeySchemaIdHeader(window.value.headers(), "IQv1 hopping window for streams " + window.key);
            }
        }

        assertTrue(streamsWindows.size() >= 2, "Should have at least 2 overlapping windows for streams, got " + streamsWindows.size());

        // Verify each streams window has correct count
        for (KeyValue<Long, ValueTimestampHeaders<Long>> window : streamsWindows) {
            long count = window.value.value();
            assertTrue(count >= 1 && count <= 2, "streams window count should be between 1 and 2, got " + count);
        }

        // Test grace period if enabled
        if (graceEnabled) {
            try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
                // Advance stream time past window end but within grace
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 12000),
                    createKey("other"), createTextLine("advance stream time"))).get();
                // Send late record for "other-late" key with old timestamp (within grace)
                producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 4000),
                    createKey("other-late"), createTextLine("late arrival event"))).get();
                producer.flush();
            }

            Thread.sleep(2000);

            store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

            // Verify "other" key
            List<KeyValue<Long, ValueTimestampHeaders<Long>>> otherWindows = new ArrayList<>();
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("other"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                while (it.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                    otherWindows.add(window);
                    assertKeySchemaIdHeader(window.value.headers(), "IQv1 hopping window for other " + window.key);
                }
            }
            assertEquals(2, otherWindows.size(), "Should have 2 windows for other key");
            for (KeyValue<Long, ValueTimestampHeaders<Long>> window : otherWindows) {
                assertEquals(1L, window.value.value(), "other window count should be 1");
            }

            // Verify "other-late" key (late-arriving record within grace)
            List<KeyValue<Long, ValueTimestampHeaders<Long>>> otherLateWindows = new ArrayList<>();
            try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("other-late"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
                while (it.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<Long>> window = it.next();
                    otherLateWindows.add(window);
                    assertKeySchemaIdHeader(window.value.headers(), "IQv1 hopping window for other-late " + window.key);
                }
            }
            assertEquals(1, otherLateWindows.size(), "Should have 1 window for other-late (earlier window closed), got " + otherLateWindows.size());
            for (KeyValue<Long, ValueTimestampHeaders<Long>> window : otherLateWindows) {
                assertEquals(1L, window.value.value(), "other-late window count should be 1");
            }
        }
        
        closeStreams(streams);

        // Verify Changelog Headers
        String changelog = "window-hop-test" + suffix + "-" + storeName + "-changelog";
        // Expected records: 2 keys (kafka + streams) with multiple overlapping windows each
        // cached scenarios: deduplicated, uncached: all updates
        // grace scenarios: additional 2 keys (other + other-late)
        int expectedMinRecords = cachingEnabled ? (graceEnabled ? 4 : 2) : (graceEnabled ? 8 : 5);
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "window-hop-cg-" + testId, expectedMinRecords + 5);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Changelog header for hopping window key");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldReduceWithTumblingWindows(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "window-reduce-input" + suffix;
        String storeName = "window-reduce-store" + suffix;

        createTopics(inputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(10);
        Duration gracePeriod = Duration.ofSeconds(5);

        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .reduce(
                (v1, v2) -> {
                    // Concatenate "line" fields from both values
                    GenericRecord combined = new GenericData.Record(valueSchema);
                    String line1 = v1.get("line").toString();
                    String line2 = v2.get("line").toString();
                    combined.put("line", line1 + "," + line2);
                    return combined;
                },
                Materialized.<GenericRecord, GenericRecord>as(
                        new WindowStoreSupplierWithHeaders(
                            Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "window-reduce-test" + suffix, cachingEnabled);

        long baseTime = 4000000L;
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send 3 records for "kafka" in same window
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("hello world from kafka"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 1000), createKey("kafka"), createTextLine("processing streams in real time"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 2000), createKey("kafka"), createTextLine("headers are preserved"))).get();
            // Send 2 records for "streams" in same window
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 3000), createKey("streams"), createTextLine("reduce first value"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 4000), createKey("streams"), createTextLine("reduce second value"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        // Verify IQv1 Fetch result
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        // Verify IQv1 Fetch result for "kafka"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed reduce result for kafka");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            String reducedLine = next.value.value().get("line").toString();
            assertTrue(reducedLine.contains("hello world from kafka") && reducedLine.contains("processing streams in real time") && reducedLine.contains("headers are preserved"),
                "kafka reduced line should contain all three values: " + reducedLine);
            assertSchemaIdHeaders(next.value.headers(), "IQv1 kafka reduce");
        }

        // Verify IQv1 Fetch for "streams"
        try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> it = store.fetch(createKey("streams"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find windowed reduce result for streams");
            KeyValue<Long, ValueTimestampHeaders<GenericRecord>> next = it.next();
            String reducedLine = next.value.value().get("line").toString();
            assertTrue(reducedLine.contains("reduce first value") && reducedLine.contains("reduce second value"),
                "streams reduced line should contain both values: " + reducedLine);
            assertSchemaIdHeaders(next.value.headers(), "IQv1 streams reduce");
        }

        closeStreams(streams);

        // Verify Changelog Headers
        String changelog = "window-reduce-test" + suffix + "-" + storeName + "-changelog";
        // 2 keys with updates, cached=deduplicated, uncached=all updates
        int expectedMinRecords = cachingEnabled ? 2 : 5;
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "window-reduce-cg-" + testId, expectedMinRecords + 3);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Changelog header for windowed reduce");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldStreamStreamsJoinWithHeaders(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String leftTopic = "join-left-input" + suffix;
        String rightTopic = "join-right-input" + suffix;
        String outputTopic = "join-output" + suffix;

        createTopics(leftTopic, rightTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde aggSerde = createAggSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration joinWindow = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(2);
        Duration storeWindowSize = joinWindow.multipliedBy(2);
        Duration retention = storeWindowSize.plus(graceEnabled ? gracePeriod : Duration.ZERO);

        // Choose join window type based on graceEnabled parameter
        JoinWindows joinWindows = graceEnabled
            ? JoinWindows.ofTimeDifferenceAndGrace(joinWindow, gracePeriod)
            : JoinWindows.ofTimeDifferenceWithNoGrace(joinWindow);

        KStream<GenericRecord, GenericRecord> leftStream = builder.stream(leftTopic, Consumed.with(keySerde, valueSerde));
        KStream<GenericRecord, GenericRecord> rightStream = builder.stream(rightTopic, Consumed.with(keySerde, valueSerde));

        String joinStoreName = "my-join" + suffix;
        String leftJoinStore = joinStoreName + "-left";
        String rightJoinStore = joinStoreName + "-right";

        // Create custom window store suppliers for join stores with headers
        WindowBytesStoreSupplier leftStoreSupplier =
            new WindowStoreSupplierWithHeaders(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    leftJoinStore,
                    retention,
                    storeWindowSize,
                    true));

        WindowBytesStoreSupplier rightStoreSupplier =
            new WindowStoreSupplierWithHeaders(
                Stores.persistentTimestampedWindowStoreWithHeaders(
                    rightJoinStore,
                    retention,
                    storeWindowSize,
                    true));

        // Inner join
        leftStream.join(
            rightStream,
            (leftValue, rightValue) -> {
                // Create aggregate record combining both sides
                GenericRecord joined = new GenericData.Record(aggSchema);
                joined.put("word", leftValue.get("line").toString() + "-" + rightValue.get("line").toString());
                joined.put("count", 1L);
                return joined;
            },
            joinWindows,
            StreamJoined.with(keySerde, valueSerde, valueSerde)
                .withThisStoreSupplier(leftStoreSupplier)
                .withOtherStoreSupplier(rightStoreSupplier)
        ).to(outputTopic, Produced.with(keySerde, aggSerde));

        String applicationId = "stream-join-test" + suffix;
        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), applicationId, cachingEnabled);

        long baseTime = 5000000L;
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(leftTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("left stream value"))).get();
            producer.send(new ProducerRecord<>(rightTopic, 0, Long.valueOf(baseTime + 1000), createKey("kafka"), createTextLine("right stream value"))).get();

            producer.send(new ProducerRecord<>(leftTopic, 0, Long.valueOf(baseTime + 3000), createKey("streams"), createTextLine("another left value"))).get();
            producer.send(new ProducerRecord<>(rightTopic, 0, Long.valueOf(baseTime + 4000), createKey("streams"), createTextLine("another right value"))).get();

            // Send non-matching record (outside join window)
            producer.send(new ProducerRecord<>(leftTopic, 0, Long.valueOf(baseTime + 20000), createKey("other"), createTextLine("other record no match"))).get();

            producer.flush();
        }

        Thread.sleep(2000); // Wait for records to be buffered

        // Verify IQv1 queries on join stores before consuming output topic
        Properties consumerProps = createConsumerProps("join-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<GenericRecord, GenericRecord>> outputRecords = new ArrayList<>();
        try (KafkaConsumer<GenericRecord, GenericRecord> consumer = new KafkaConsumer<>(consumerProps, keySerde.deserializer(), aggSerde.deserializer())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            ConsumerRecords<GenericRecord, GenericRecord> records = consumer.poll(java.time.Duration.ofSeconds(10));
            records.forEach(outputRecords::add);
        }

        assertEquals(2, outputRecords.size(), "Should have 2 join results (kafka and streams)");

        // Collect results by key for verification
        Map<String, ConsumerRecord<GenericRecord, GenericRecord>> resultsByKey = new HashMap<>();
        for (ConsumerRecord<GenericRecord, GenericRecord> record : outputRecords) {
            String keyWord = record.key().get("word").toString();
            resultsByKey.put(keyWord, record);
        }

        // Verify "kafka" join result
        assertTrue(resultsByKey.containsKey("kafka"), "Should have join result for kafka key");
        ConsumerRecord<GenericRecord, GenericRecord> kafkaResult = resultsByKey.get("kafka");
        assertEquals("kafka", kafkaResult.key().get("word").toString(), "Key should be kafka");
        assertEquals("left stream value-right stream value", kafkaResult.value().get("word").toString(),
            "Joined value should be 'left stream value-right stream value'");
        assertEquals(1L, kafkaResult.value().get("count"), "Count should be 1");
        assertSchemaIdHeaders(kafkaResult.headers(), "Join output for kafka");

        // Verify "streams" join result
        assertTrue(resultsByKey.containsKey("streams"), "Should have join result for streams key");
        ConsumerRecord<GenericRecord, GenericRecord> streamsResult = resultsByKey.get("streams");
        assertEquals("streams", streamsResult.key().get("word").toString(), "Key should be streams");
        assertEquals("another left value-another right value", streamsResult.value().get("word").toString(),
            "Joined value should be 'another left value-another right value'");
        assertEquals(1L, streamsResult.value().get("count"), "Count should be 1");
        assertSchemaIdHeaders(streamsResult.headers(), "Join output for streams");

        // Verify "other" key did not join
        assertFalse(resultsByKey.containsKey("other"), "Should not have joined result for other key");

        closeStreams(streams);

        // Verify changelog topics for join stores
        String leftChangelog = applicationId + "-" + leftJoinStore + "-changelog";
        String rightChangelog = applicationId + "-" + rightJoinStore + "-changelog";

        // Verify left join store changelog
        List<ConsumerRecord<byte[], byte[]>> leftChangelogRecords =
            consumeRawChangelog(leftChangelog, "join-left-changelog-cg-" + testId, 3);
        assertTrue(leftChangelogRecords.size() >= 1, "Left join changelog should have at least 1 record, got " + leftChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : leftChangelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Left join store changelog");
            }
        }

        // Verify right join store changelog
        List<ConsumerRecord<byte[], byte[]>> rightChangelogRecords =
            consumeRawChangelog(rightChangelog, "join-right-changelog-cg-" + testId, 3);
        assertTrue(rightChangelogRecords.size() >= 1, "Right join changelog should have at least 1 record, got " + rightChangelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : rightChangelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Right join store changelog");
            }
        }
    }

    @ParameterizedTest
    @MethodSource("cacheAndGraceParams")
    public void shouldSuppressWithHeaders(boolean cachingEnabled, boolean graceEnabled) throws Exception {
        String testId = UUID.randomUUID().toString().substring(0, 8);
        String suffix = cachingEnabled ? "-cached" : "-uncached";
        suffix += graceEnabled ? "-grace" : "-nograce";
        suffix += "-" + testId;
        String inputTopic = "suppress-input" + suffix;
        String outputTopic = "suppress-output" + suffix;
        String storeName = "suppress-store" + suffix;

        createTopics(inputTopic, outputTopic);
        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        Duration windowSize = Duration.ofSeconds(5);
        Duration gracePeriod = Duration.ofSeconds(2);

        // Choose window type based on graceEnabled parameter
        TimeWindows timeWindows = graceEnabled
            ? TimeWindows.ofSizeAndGrace(windowSize, gracePeriod)
            : TimeWindows.ofSizeWithNoGrace(windowSize);

        // Count with suppression - results only emitted when window closes
        builder.stream(inputTopic, Consumed.with(keySerde, valueSerde))
            .groupByKey()
            .windowedBy(timeWindows)
            .count(Materialized.<GenericRecord, Long>as(
                    new WindowStoreSupplierWithHeaders(
                        Stores.persistentTimestampedWindowStoreWithHeaders(storeName, Duration.ofMinutes(5), windowSize, false)))
                .withKeySerde(keySerde))
            .suppress(Suppressed.untilWindowCloses(
                Suppressed.BufferConfig.unbounded()))
            .toStream()
            .to(outputTopic, Produced.with(
                new WindowedSerdes.TimeWindowedSerde<>(keySerde, windowSize.toMillis()),
                Serdes.Long()));

        KafkaStreams streams = startStreamsAndAwaitRunning(builder.build(), "suppress-test" + suffix, cachingEnabled,
            Collections.singletonMap(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L));

        long baseTime = 6000000L;

        // Send records within first window - should be suppressed (not emitted yet)
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime), createKey("kafka"), createTextLine("quick brown fox jumps"))).get();
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 1000), createKey("kafka"), createTextLine("windowed by time"))).get();
            producer.flush();
        }

        Thread.sleep(2000);

        // Verify no output yet (suppressed)
        Properties consumerProps = createConsumerProps("suppress-output-cg-" + testId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<byte[], byte[]>> outputRecords = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps,
            ByteArrayDeserializer.class.newInstance(),
            ByteArrayDeserializer.class.newInstance())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(java.time.Duration.ofSeconds(3));
            records.forEach(outputRecords::add);
        }

        assertEquals(0, outputRecords.size(), "Should have no output yet (suppressed until window closes)");

        // Advance stream time past window close + grace period to trigger suppression release
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send record far in future to advance stream time
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 10000), createKey("other"), createTextLine("advance stream time"))).get();
            producer.flush();
        }

        Thread.sleep(3000);

        // Verify suppressed results are emitted with headers
        outputRecords.clear();
        Properties consumerProps2 = createConsumerProps("suppress-output-cg2-" + testId);
        consumerProps2.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps2,
            ByteArrayDeserializer.class.newInstance(),
            ByteArrayDeserializer.class.newInstance())) {
            consumer.subscribe(Collections.singletonList(outputTopic));
            ConsumerRecords<byte[], byte[]> records = consumer.poll(java.time.Duration.ofSeconds(5));
            records.forEach(outputRecords::add);
        }

        assertEquals(1, outputRecords.size(), "Should have 2 suppressed result released, got " + outputRecords.size());

        // Verify headers in suppressed output
        for (ConsumerRecord<byte[], byte[]> record : outputRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Suppressed output");
            }
        }

        // Verify IQv1 query shows headers
        ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<Long>> store = streams.store(
            StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));

        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("kafka"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find kafka window in store");
            KeyValue<Long, ValueTimestampHeaders<Long>> result = it.next();
            assertEquals(2L, result.value.value(), "kafka count should be 2");
            assertKeySchemaIdHeader(result.value.headers(), "IQv1 suppressed store");
        }

        // Test null value handling
        try (KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(createProducerProps())) {
            // Send null value for "kafka" key - should be skipped, not counted
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 20000), createKey("nullvalue"),  createTextLine("null value"))).get();
            // Send normal value for "streams" key
            producer.send(new ProducerRecord<>(inputTopic, 0, Long.valueOf(baseTime + 24000), createKey("nullvalue"), (GenericRecord) null)).get();
            producer.flush();
        }

        Thread.sleep(2000);

        // Re-fetch store
        store = streams.store(StoreQueryParameters.fromNameAndType(storeName, new TimestampedWindowStoreWithHeadersType<>()));
        try (WindowStoreIterator<ValueTimestampHeaders<Long>> it = store.fetch(createKey("nullvalue"), Instant.ofEpochMilli(0), Instant.ofEpochMilli(Long.MAX_VALUE))) {
            assertTrue(it.hasNext(), "Should find nullvalue in store");
            KeyValue<Long, ValueTimestampHeaders<Long>> result = it.next();
            assertEquals(2L, result.value.value(), "nullvalue count should be 1 (null value should be counted as an event, not skipped)");
            assertKeySchemaIdHeader(result.value.headers(), "IQv1 null value handling in suppressed store");
        }

        closeStreams(streams);

        // Verify changelog headers
        String changelog = "suppress-test" + suffix + "-" + storeName + "-changelog";
        int expectedMinRecords = cachingEnabled ? 3 : 5; // kafka updates + other
        List<ConsumerRecord<byte[], byte[]>> changelogRecords = consumeRawChangelog(changelog, "suppress-changelog-cg-" + testId, expectedMinRecords);

        assertTrue(changelogRecords.size() >= expectedMinRecords,
            "Should have at least " + expectedMinRecords + " changelog records, got " + changelogRecords.size());
        for (ConsumerRecord<byte[], byte[]> record : changelogRecords) {
            if (record.value() != null) {
                assertKeySchemaIdHeader(record.headers(), "Changelog with suppression");
            }
        }
    }

    // Custom Window Store Type
    private static class TimestampedWindowStoreWithHeadersType<K, V>
        implements QueryableStoreType<ReadOnlyWindowStore<K, ValueTimestampHeaders<V>>> {
        @Override public boolean accepts(StateStore s) {
            return s instanceof TimestampedWindowStoreWithHeaders && s instanceof ReadOnlyWindowStore;
        }
        @Override public ReadOnlyWindowStore<K, ValueTimestampHeaders<V>> create(StateStoreProvider p, String n) {
            return new CompositeReadOnlyWindowStore<>(p, this, n);
        }
    }

    /**
     * Wrapper for WindowBytesStoreSupplier that implements HeadersBytesStoreSupplier.
     * This is needed because RocksDbWindowBytesStoreSupplier doesn't implement HeadersBytesStoreSupplier
     * even when configured to create header-aware stores, causing the DSL to use the wrong builder.
     */
    private static class WindowStoreSupplierWithHeaders implements WindowBytesStoreSupplier, HeadersBytesStoreSupplier {
        private final WindowBytesStoreSupplier delegate;

        WindowStoreSupplierWithHeaders(WindowBytesStoreSupplier delegate) {
            this.delegate = delegate;
        }

        @Override public String name() { return delegate.name(); }
        @Override public WindowStore<Bytes, byte[]> get() { return delegate.get(); }
        @Override public String metricsScope() { return delegate.metricsScope(); }
        @Override public long segmentIntervalMs() { return delegate.segmentIntervalMs(); }
        @Override public long windowSize() { return delegate.windowSize(); }
        @Override public boolean retainDuplicates() { return delegate.retainDuplicates(); }
        @Override public long retentionPeriod() { return delegate.retentionPeriod(); }
    }

    private void createTopics(String... topicNames) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(Arrays.stream(topicNames).map(n -> new NewTopic(n, 1, (short) 1)).collect(Collectors.toList())).all().get(30, TimeUnit.SECONDS);
        }
    }

    private GenericAvroSerde createKeySerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, true);
        return serde;
    }

    private GenericAvroSerde createValueSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        return serde;
    }

    private GenericAvroSerde createAggSerde() {
        GenericAvroSerde serde = new GenericAvroSerde();
        Map<String, Object> config = new HashMap<>();
        config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        config.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        serde.configure(config, false);
        return serde;
    }

    private Properties createProducerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(AbstractKafkaSchemaSerDeConfig.KEY_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());
        return props;
    }

    private Properties createConsumerProps(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        return props;
    }

    private KafkaStreams startStreamsAndAwaitRunning(org.apache.kafka.streams.Topology topology, String appId, boolean cachingEnabled) throws Exception {
        return startStreamsAndAwaitRunning(topology, appId, cachingEnabled, Collections.emptyMap());
    }

    private KafkaStreams startStreamsAndAwaitRunning(org.apache.kafka.streams.Topology topology, String appId, boolean cachingEnabled, Map<String, Object> extraProps) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(StreamsConfig.DSL_STORE_FORMAT_CONFIG, StreamsConfig.DSL_STORE_FORMAT_HEADERS);
        if (!cachingEnabled) props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.putAll(extraProps);

        KafkaStreams streams = new KafkaStreams(topology, props);
        CountDownLatch latch = new CountDownLatch(1);
        streams.setStateListener((n, o) -> { if (n == KafkaStreams.State.RUNNING) latch.countDown(); });
        streams.start();
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        return streams;
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
        Header h = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
        assertNotNull(h, context + ": Missing key schema ID");
        assertEquals(17, h.value().length);
    }

    private GenericRecord createKey(String word) {
        GenericRecord r = new GenericData.Record(keySchema);
        r.put("word", word);
        return r;
    }

    private GenericRecord createTextLine(String line) {
        GenericRecord r = new GenericData.Record(valueSchema);
        r.put("line", line);
        return r;
    }

    private List<ConsumerRecord<byte[], byte[]>> consumeRawChangelog(String topic, String group, int count) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        List<ConsumerRecord<byte[], byte[]>> results = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(Collections.singletonList(topic));
            long end = System.currentTimeMillis() + 15000;
            while (results.size() < count && System.currentTimeMillis() < end) {
                for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) results.add(r);
            }
        }
        return results;
    }

    private void closeStreams(KafkaStreams streams) { if (streams != null) streams.close(); }
}