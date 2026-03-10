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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link TimestampedWindowStoreWithHeaders} that verifies windowed store
 * operations (put, fetch, aggregation) work correctly with header-based schema ID transport.
 */
public class TimestampedWindowStoreWithHeadersIntegrationTest extends ClusterTestHarness {

    private static final String INPUT_TOPIC = "events-input";
    private static final String OUTPUT_TOPIC = "windowed-output";
    private static final String STORE_NAME = "event-window-store";
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(5);
    private static final Duration RETENTION_PERIOD = Duration.ofHours(1);

    private static final String KEY_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"EventKey\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"eventId\",\"type\":\"string\"}"
            + "]"
            + "}";

    private static final String VALUE_SCHEMA_JSON =
        "{"
            + "\"type\":\"record\","
            + "\"name\":\"EventValue\","
            + "\"namespace\":\"io.confluent.kafka.streams.integration\","
            + "\"fields\":["
            + "  {\"name\":\"count\",\"type\":\"long\"},"
            + "  {\"name\":\"operation\",\"type\":\"string\",\"default\":\"PUT\"}"
            + "]"
            + "}";

    private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
    private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);

    public TimestampedWindowStoreWithHeadersIntegrationTest() {
        super(1, true);
    }

    @Test
    public void shouldPerformWindowedOperationsWithHeaders() throws Exception {
        createTopics(INPUT_TOPIC, OUTPUT_TOPIC);

        GenericAvroSerde keySerde = createKeySerde();
        GenericAvroSerde valueSerde = createValueSerde();

        StreamsBuilder builder = new StreamsBuilder();
        builder
            .addStateStore(
                Stores.timestampedWindowStoreWithHeadersBuilder(
                    Stores.persistentTimestampedWindowStoreWithHeaders(
                        STORE_NAME, RETENTION_PERIOD, WINDOW_SIZE, false),
                    keySerde,
                    valueSerde))
            .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
            .process(() -> new WindowedEventProcessor(STORE_NAME), STORE_NAME)
            .to(OUTPUT_TOPIC, Produced.with(keySerde, valueSerde));

        KafkaStreams streams = null;
        try {
            streams = startStreamsAndAwaitRunning(builder.build(), "window-store-integration-test");

            GenericRecord event1Key = createKey("event-1");

            try (KafkaProducer<GenericRecord, GenericRecord> producer =
                     new KafkaProducer<>(createProducerProps())) {

                // Test 1: PUT at t=1min (window 0-5min)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 60000L, event1Key, createValue(1L, "PUT"))).get();

                // Test 2: PUT at t=2min (same window, should aggregate to count=2)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 120000L, event1Key, createValue(1L, "PUT"))).get();

                // Test 3: PUT at t=6min (new window 5-10min, count=1)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 360000L, event1Key, createValue(1L, "PUT"))).get();

                // Test 4: FETCH at t=1min (fetch from window 0-5min)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 60000L, event1Key, createValue(0L, "FETCH"))).get();

                // Test 5: FETCH_RANGE - fetch all windows for event-1 from 0 to 10min (should return 2 windows)
                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event1Key, createValue(0L, "FETCH_RANGE"))).get();

                // Test 6: FETCH_ALL - fetch all entries in the store from 0 to 10min
//                producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 0L, event1Key, createValue(0L, "FETCH_ALL"))).get();

                producer.flush();
            }

            // Expect 6 output records: 3 PUTs + 1 FETCH + 2 FETCH_RANGE
            List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                consumeRecords(OUTPUT_TOPIC, "window-store-test-consumer", 6);

            assertEquals(6, results.size(), "Should have 6 output records");

            // Verify PUT 1 (window 0-5min, count=1)
            assertEquals("event-1", results.get(0).key().get("eventId").toString());
            assertEquals(1L, results.get(0).value().get("count"));
            assertSchemaIdHeaders(results.get(0), "PUT 1");

            // Verify PUT 2 (same window, aggregated count=2)
            assertEquals("event-1", results.get(1).key().get("eventId").toString());
            assertEquals(2L, results.get(1).value().get("count"));
            assertSchemaIdHeaders(results.get(1), "PUT 2 aggregated");

            // Verify PUT 3 (new window 5-10min, count=1)
            assertEquals("event-1", results.get(2).key().get("eventId").toString());
            assertEquals(1L, results.get(2).value().get("count"));
            assertSchemaIdHeaders(results.get(2), "PUT 3 new window");

            // Verify FETCH (from window 0-5min, count=2)
            assertEquals("event-1", results.get(3).key().get("eventId").toString());
            assertEquals(2L, results.get(3).value().get("count"));
            assertSchemaIdHeaders(results.get(3), "FETCH");

            // Verify FETCH_RANGE (2 windows for event-1: 0-5min and 5-10min)
            assertEquals("event-1", results.get(4).key().get("eventId").toString());
            assertEquals(2L, results.get(4).value().get("count"));
            assertSchemaIdHeaders(results.get(4), "FETCH_RANGE window1");

            assertEquals("event-1", results.get(5).key().get("eventId").toString());
            assertEquals(1L, results.get(5).value().get("count"));
            assertSchemaIdHeaders(results.get(5), "FETCH_RANGE window2");


            // Query store via IQv1 to verify state
            ReadOnlyWindowStore<GenericRecord, ValueTimestampHeaders<GenericRecord>> store =
                streams.store(
                    StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.windowStore()));

            assertNotNull(store, "Store should be accessible via IQv1");

            // Verify window 0-5min via IQv1 (count=2)
            long window1Start = 0L;
            ValueTimestampHeaders<GenericRecord> window1Result = store.fetch(event1Key, window1Start);
            assertNotNull(window1Result, "IQv1: window 0-5min should have data");
            assertEquals(2L, window1Result.value().get("count"), "IQv1: window 0-5min count should be 2");
            assertSchemaIdHeaders(window1Result.headers(), "IQv1 window 0-5min");

            // Verify window 5-10min via IQv1 (count=1)
            long window2Start = 300000L; // 5min
            ValueTimestampHeaders<GenericRecord> window2Result = store.fetch(event1Key, window2Start);
            assertNotNull(window2Result, "IQv1: window 5-10min should have data");
            assertEquals(1L, window2Result.value().get("count"), "IQv1: window 5-10min count should be 1");
            assertSchemaIdHeaders(window2Result.headers(), "IQv1 window 5-10min");

        } finally {
            closeStreams(streams);
        }
    }

    /**
     * Processor that aggregates events in time windows using TimestampedWindowStoreWithHeaders.
     */
    private static class WindowedEventProcessor
        implements Processor<GenericRecord, GenericRecord, GenericRecord, GenericRecord> {

        private final String storeName;
        private ProcessorContext<GenericRecord, GenericRecord> context;
        private TimestampedWindowStoreWithHeaders<GenericRecord, GenericRecord> store;

        public WindowedEventProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext<GenericRecord, GenericRecord> context) {
            this.context = context;
            this.store = context.getStateStore(storeName);
        }

        @Override
        public void process(Record<GenericRecord, GenericRecord> record) {
            String operation = record.value().get("operation").toString();

            switch (operation) {
                case "PUT":
                    handlePut(record);
                    break;
                case "FETCH":
                    handleFetch(record);
                    break;
                case "FETCH_RANGE":
                    handleFetchRange(record);
                    break;
                case "FETCH_ALL":
                    handleFetchAll(record);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown operation: " + operation);
            }
        }

        private void handlePut(Record<GenericRecord, GenericRecord> record) {
            long windowStart = (record.timestamp() / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();

            ValueTimestampHeaders<GenericRecord> existingRecord = store.fetch(record.key(), windowStart);

            long newCount = (Long) record.value().get("count");
            if (existingRecord != null && existingRecord.value() != null) {
                newCount += (Long) existingRecord.value().get("count");
            }

            GenericRecord aggregatedValue = new GenericData.Record(record.value().getSchema());
            aggregatedValue.put("count", newCount);
            aggregatedValue.put("operation", "PUT");

            ValueTimestampHeaders<GenericRecord> toStore =
                ValueTimestampHeaders.make(aggregatedValue, record.timestamp(), record.headers());
            store.put(record.key(), toStore, windowStart);

            ValueTimestampHeaders<GenericRecord> stored = store.fetch(record.key(), windowStart);
            context.forward(new Record<>(
                record.key(), stored.value(), stored.timestamp(), stored.headers()));
        }

        private void handleFetch(Record<GenericRecord, GenericRecord> record) {
            long windowStart = (record.timestamp() / WINDOW_SIZE.toMillis()) * WINDOW_SIZE.toMillis();

            ValueTimestampHeaders<GenericRecord> fetched = store.fetch(record.key(), windowStart);
            if (fetched != null) {
                context.forward(new Record<>(
                    record.key(), fetched.value(), fetched.timestamp(), fetched.headers()));
            }
        }

        private void handleFetchRange(Record<GenericRecord, GenericRecord> record) {
            // Fetch all windows for this key from 0 to 10 minutes
            try (WindowStoreIterator<ValueTimestampHeaders<GenericRecord>> iterator =
                     store.fetch(record.key(), Instant.ofEpochMilli(0), Instant.ofEpochMilli(600000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Long, ValueTimestampHeaders<GenericRecord>> entry = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = entry.value;
                    context.forward(new Record<>(
                        record.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
        }

        private void handleFetchAll(Record<GenericRecord, GenericRecord> record) {
            // Fetch all entries in the store (for testing purposes)
            try (KeyValueIterator<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> iterator =
                     store.fetchAll(Instant.ofEpochMilli(0), Instant.ofEpochMilli(600000L))) {
                while (iterator.hasNext()) {
                    KeyValue<Windowed<GenericRecord>, ValueTimestampHeaders<GenericRecord>> next = iterator.next();
                    ValueTimestampHeaders<GenericRecord> value = next.value;
                    context.forward(new Record<>(
                        record.key(), value.value(), value.timestamp(), value.headers()));
                }
            }
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

    private KafkaStreams startStreamsAndAwaitRunning(Topology topology, String appId) throws Exception {
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

    private void assertSchemaIdHeaders(ConsumerRecord<GenericRecord, GenericRecord> record, String context) {
        assertSchemaIdHeaders(record.headers(), context);
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

    private GenericRecord createKey(String eventId) {
        GenericRecord key = new GenericData.Record(keySchema);
        key.put("eventId", eventId);
        return key;
    }

    private GenericRecord createValue(long count, String operation) {
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("count", count);
        value.put("operation", operation);
        return value;
    }
}
