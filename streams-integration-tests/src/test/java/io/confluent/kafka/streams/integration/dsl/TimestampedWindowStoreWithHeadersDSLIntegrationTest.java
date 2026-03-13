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
 import org.apache.kafka.streams.KafkaStreams;
 import org.apache.kafka.streams.StreamsBuilder;
 import org.apache.kafka.streams.StreamsConfig;
 import org.apache.kafka.streams.kstream.Consumed;
 import org.apache.kafka.streams.kstream.Grouped;
 import org.apache.kafka.streams.kstream.Materialized;
 import org.apache.kafka.streams.kstream.Produced;
 import org.apache.kafka.streams.kstream.TimeWindows;
 import org.apache.kafka.streams.state.Stores;
 import org.apache.kafka.streams.state.WindowBytesStoreSupplier;
 import org.junit.jupiter.api.Test;

 /**
  * DSL-based integration test for {@link TimestampedWindowStoreWithHeadersDSLIntegrationTest} that verifies
  * windowed aggregations work correctly with header-based schema ID transport.
  */
 public class TimestampedWindowStoreWithHeadersDSLIntegrationTest extends ClusterTestHarness {

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
             + "  {\"name\":\"count\",\"type\":\"long\"}"
             + "]"
             + "}";

     private static final String AGG_SCHEMA_JSON =
         "{"
             + "\"type\":\"record\","
             + "\"name\":\"AggregatedValue\","
             + "\"namespace\":\"io.confluent.kafka.streams.integration\","
             + "\"fields\":["
             + "  {\"name\":\"count\",\"type\":\"long\"}"
             + "]"
             + "}";

     private final Schema keySchema = new Schema.Parser().parse(KEY_SCHEMA_JSON);
     private final Schema valueSchema = new Schema.Parser().parse(VALUE_SCHEMA_JSON);
     private final Schema aggSchema = new Schema.Parser().parse(AGG_SCHEMA_JSON);

     public TimestampedWindowStoreWithHeadersDSLIntegrationTest() {
         super(1, true);
     }

     @Test
     public void shouldAggregateWithHeadersUsingDSL() throws Exception {
         createTopics(INPUT_TOPIC, OUTPUT_TOPIC);

         GenericAvroSerde keySerde = createKeySerde();
         GenericAvroSerde valueSerde = createValueSerde();
         GenericAvroSerde aggSerde = createAggSerde();

         // Build DSL topology with windowed aggregation
         StreamsBuilder builder = new StreamsBuilder();

         // Create the store supplier for TimestampedWindowStoreWithHeaders
         WindowBytesStoreSupplier storeSupplier =
             Stores.persistentTimestampedWindowStoreWithHeaders(
                 STORE_NAME, RETENTION_PERIOD, WINDOW_SIZE, false);

         builder
             .stream(INPUT_TOPIC, Consumed.with(keySerde, valueSerde))
             .groupByKey(Grouped.with(keySerde, valueSerde))
             .windowedBy(TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE))
             .aggregate(
                 // Initializer: create initial aggregate
                 () -> {
                     GenericRecord initial = new GenericData.Record(aggSchema);
                     initial.put("count", 0L);
                     return initial;
                 },
                 // Aggregator: accumulate values
                 (key, value, aggregate) -> {
                     GenericRecord result = new GenericData.Record(aggSchema);
                     long currentCount = (Long) aggregate.get("count");
                     long newCount = (Long) value.get("count");
                     result.put("count", currentCount + newCount);
                     return result;
                 },
                 // Materialized with TimestampedWindowStoreWithHeaders
                 Materialized.<GenericRecord, GenericRecord>as(storeSupplier)
                     .withKeySerde(keySerde)
                     .withValueSerde(aggSerde)
             )
             .toStream()
             .selectKey((windowedKey, value) -> windowedKey.key())
             .to(OUTPUT_TOPIC, Produced.with(keySerde, aggSerde));

         KafkaStreams streams = null;
         try {
             streams = startStreamsAndAwaitRunning(builder.build(), "dsl-window-store-test");

             try (KafkaProducer<GenericRecord, GenericRecord> producer =
                      new KafkaProducer<>(createProducerProps())) {

                 GenericRecord event1Key = createKey("event-1");
                 GenericRecord event2Key = createKey("event-2");

                 // event-1: count=5 at t=1min (window 0-5min)
                 producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 60000L, event1Key,
                     createValue(5L))).get();

                 // event-1: count=3 at t=2min (same window, aggregates to 8)
                 producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 120000L, event1Key,
                     createValue(3L))).get();

                 // event-1: count=7 at t=6min (window 5-10min)
                 producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 360000L, event1Key,
                     createValue(7L))).get();

                 // event-2: count=10 at t=2min (window 0-5min)
                 producer.send(new ProducerRecord<>(INPUT_TOPIC, null, 120000L, event2Key,
                     createValue(10L))).get();

                 producer.flush();
             }

             // Expect 4 output records (one per input event, showing progressive aggregation)
             List<ConsumerRecord<GenericRecord, GenericRecord>> results =
                 consumeRecords(OUTPUT_TOPIC, "dsl-test-consumer", 4);

             assertEquals(4, results.size(), "Should have 4 output records");

             // Verify all records have schema ID headers
             for (int i = 0; i < results.size(); i++) {
                 assertSchemaIdHeaders(results.get(i), "Record " + i);
             }

             // Verify final aggregated values
             // Find event-1's final aggregation for window 0-5min (count=8)
             ConsumerRecord<GenericRecord, GenericRecord> event1Window1Final = results.stream()
                 .filter(r -> "event-1".equals(r.key().get("eventId").toString()))
                 .filter(r -> (Long) r.value().get("count") == 8L)
                 .findFirst()
                 .orElse(null);

             assertNotNull(event1Window1Final, "Should have event-1 aggregation with count 8");

             // Find event-1's aggregation for window 5-10min (count=7)
             ConsumerRecord<GenericRecord, GenericRecord> event1Window2 = results.stream()
                 .filter(r -> "event-1".equals(r.key().get("eventId").toString()))
                 .filter(r -> (Long) r.value().get("count") == 7L)
                 .findFirst()
                 .orElse(null);

             assertNotNull(event1Window2, "Should have event-1 aggregation with count 7");

             // Find event-2's aggregation (count=10)
             ConsumerRecord<GenericRecord, GenericRecord> event2Final = results.stream()
                 .filter(r -> "event-2".equals(r.key().get("eventId").toString()))
                 .filter(r -> (Long) r.value().get("count") == 10L)
                 .findFirst()
                 .orElse(null);

             assertNotNull(event2Final, "Should have event-2 aggregation with count 10");

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

     private GenericAvroSerde createAggSerde() {
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

     private List<ConsumerRecord<GenericRecord, GenericRecord>> consumeRecords(
         String topic, String groupId, int expectedCount) {
         List<ConsumerRecord<GenericRecord, GenericRecord>> results = new ArrayList<>();
         try (KafkaConsumer<GenericRecord, GenericRecord> consumer =
                  new KafkaConsumer<>(createConsumerProps(groupId))) {
             consumer.subscribe(Collections.singletonList(topic));
             long deadline = System.currentTimeMillis() + 30_000;
             while (results.size() < expectedCount && System.currentTimeMillis() < deadline) {
                 ConsumerRecords<GenericRecord, GenericRecord> records =
                     consumer.poll(Duration.ofMillis(500));
                 for (ConsumerRecord<GenericRecord, GenericRecord> record : records) {
                     results.add(record);
                 }
             }
         }
         return results;
     }

     private void assertSchemaIdHeaders(ConsumerRecord<GenericRecord, GenericRecord> record, String
         context) {
         Headers headers = record.headers();
         Header keySchemaIdHeader = headers.lastHeader(SchemaId.KEY_SCHEMA_ID_HEADER);
         assertNotNull(keySchemaIdHeader, context + ": should have __key_schema_id header");

         Header valueSchemaIdHeader = headers.lastHeader(SchemaId.VALUE_SCHEMA_ID_HEADER);
         assertNotNull(valueSchemaIdHeader, context + ": should have __value_schema_id header");
     }

     private GenericRecord createKey(String eventId) {
         GenericRecord key = new GenericData.Record(keySchema);
         key.put("eventId", eventId);
         return key;
     }

     private GenericRecord createValue(long count) {
         GenericRecord value = new GenericData.Record(valueSchema);
         value.put("count", count);
         return value;
     }
 }