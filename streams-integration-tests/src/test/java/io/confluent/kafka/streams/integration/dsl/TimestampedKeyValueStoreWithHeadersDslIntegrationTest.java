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

import static org.apache.kafka.streams.KeyValue.pair;
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
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * DSL integration test for {@link TimestampedKeyValueStoreWithHeaders}.
 */
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
     * Verifies `groupBy()` and `count()` works correctly use headers-aware stores.
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
            .flatMapValues(value ->
                Arrays.asList(value.get("line").toString().toLowerCase().split("\\W+")))
            .groupBy((key, word) -> {
                GenericRecord wordKey = new GenericData.Record(keySchema);
                wordKey.put("word", word);
                return wordKey;
            }, Grouped.with(keySerde, Serdes.String()))
            .aggregate(
                () -> {
                    GenericRecord init = new GenericData.Record(aggSchema);
                    init.put("word", "");
                    init.put("count", 0L);
                    return init;
                },
                (key, value, agg) -> {
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
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("key1"), createTextLine("hello kafka streams"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("key2"), createTextLine("all streams lead to kafka"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("key3"), createTextLine("join kafka summit"))).get();
                producer.flush();
            }

            // Without caching: 11 records. With caching: between 8 and 11.
            int minExpected = 8;
            int maxExpected = 11;
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
                (oldValue, newValue) -> newValue,
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
        String storeName = "dsl-mapvalues-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();
        GenericAvroSerde mappedSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde))
            .mapValues(value -> {
                GenericRecord mapped = new GenericData.Record(mapValueSchema);
                mapped.put("firstWord", value.get("line").toString().split("\\W+")[0]);
                mapped.put("count", (long) value.get("line").toString().length());
                return mapped;
            }, Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(storeName))
                .withKeySerde(keySerde)
                .withValueSerde(mappedSerde))
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
                    StoreQueryParameters.fromNameAndType(storeName,
                        new TimestampedKeyValueStoreWithHeadersType<>()));
            assertNotNull(store, "Store should be accessible via IQv1");

            ValueTimestampHeaders<GenericRecord> helloResult = store.get(createKey("hello"));
            assertNotNull(helloResult, "IQv1: hello should exist in store");
            assertEquals("hello", helloResult.value().get("firstWord").toString(),
                "IQv1: hello mapped word");
            assertEquals(19L, helloResult.value().get("count"),
                "IQv1: hello line length should be 20");
            assertSchemaIdHeaders(helloResult.headers(), "IQv1 get hello");

            ValueTimestampHeaders<GenericRecord> kafkaResult = store.get(createKey("streams"));
            assertNotNull(kafkaResult, "IQv1: streams should exist in store");
            assertEquals("kafka", kafkaResult.value().get("firstWord").toString(),
                "IQv1: streams mapped word");
            assertEquals(13L, kafkaResult.value().get("count"),
                "IQv1: streams line length should be 13");
            assertSchemaIdHeaders(kafkaResult.headers(), "IQv1 get streams");

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
        String filterNotStoreName = "dsl-filter-not-store" + suffix;

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde))
            .filterNot((key, value) -> value.get("line").toString().contains("kafka"),
                Materialized.<GenericRecord, GenericRecord>as(
                        Stores.persistentTimestampedKeyValueStoreWithHeaders(filterNotStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
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

            // IQv1 verification
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
            builder.table(namesTopic, Consumed.with(keySerde, valueSerde));
        KTable<GenericRecord, GenericRecord> agesTable =
            builder.table(agesTopic, Consumed.with(keySerde, valueSerde));

        // inner join
        namesTable.join(agesTable,
                (name, age) -> {
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

            // IQv1 verification
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
