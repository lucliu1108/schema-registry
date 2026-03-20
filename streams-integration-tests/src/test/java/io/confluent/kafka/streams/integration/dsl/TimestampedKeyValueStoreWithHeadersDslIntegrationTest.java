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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.junit.jupiter.api.Test;

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

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    public TimestampedKeyValueStoreWithHeadersDslIntegrationTest() {
        super(1, true);
    }

    /**
     * Verifies `groupBy()` and `count()` works correctly use headers-aware stores.
     */
    @Test
    public void shouldGroupAndAggregateWithHeaders() throws Exception {
        String inputTopic = "dsl-count-supplier-input";
        String outputTopic = "dsl-count-supplier-output";
        String storeName = "dsl-count-supplier-store";

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
            streams = startStreamsAndAwaitRunning(builder.build(), "dsl-count-supplier-test");

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

            // 3+5+3 = 11 words.
            List<ConsumerRecord<GenericRecord, Long>> results =
                consumeLongValueRecords(outputTopic, "dsl-count-supplier-consumer", 11);

            assertEquals(11, results.size(), "Should have 11 output records");

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
     * Verify `filter()` works correctly with a headers-aware store.
     */
    @Test
    public void shouldFilterWithHeaders() throws Exception {
        String inputTopic = "dsl-filter-input";
        String outputTopic = "dsl-filter-output";
        String filterStoreName = "dsl-filter-store";

        createTopics(inputTopic, outputTopic);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder.table(inputTopic, Consumed.with(keySerde, valueSerde))
            .filter((key, value) -> value.get("line").toString().length() > 10,
                Materialized.<GenericRecord, GenericRecord>as(
                    Stores.persistentTimestampedKeyValueStoreWithHeaders(filterStoreName))
                    .withKeySerde(keySerde)
                    .withValueSerde(valueSerde))
            .toStream()
            .to(outputTopic, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "dsl-filter-test");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("long"), createTextLine("this is a long long line"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("long2"), createTextLine("this is another long line"))).get();
                producer.send(new ProducerRecord<>(inputTopic,
                    createKey("short"), createTextLine("line"))).get();
                producer.flush();
            }

            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(outputTopic, "dsl-filter-consumer", 2);

            assertEquals(2, results.size(), "Only the long lines should pass filter");
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
            assertKeySchemaIdHeader(longResult.headers(), "filter store: long");
            ValueTimestampHeaders<GenericRecord> longResult2 = filterStore.get(createKey("long2"));
            assertNotNull(longResult2, "filter store: 'long2' should exist");
            assertKeySchemaIdHeader(longResult2.headers(), "filter store: long2");

            ValueTimestampHeaders<GenericRecord> shortResult = filterStore.get(createKey("short"));
            assertTrue(shortResult == null || shortResult.value() == null,
                "filter store: 'short' should be filtered out");

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

    private Properties createStreamsProps(String appId) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, restApp.restConnect);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
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
        org.apache.kafka.streams.Topology topology, String appId) throws Exception {
        CountDownLatch startedLatch = new CountDownLatch(1);
        KafkaStreams streams = new KafkaStreams(topology, createStreamsProps(appId));
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
