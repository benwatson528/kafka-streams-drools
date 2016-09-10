package uk.co.hadoopathome.kafkastreams.integration; /**
 * Copyright 2016 Confluent Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * This class is adapted from https://github.com/JohnReedLOL/kafka-streams.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import uk.co.hadoopathome.kafkastreams.drools.DroolsRulesApplier;
import uk.co.hadoopathome.kafkastreams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import uk.co.hadoopathome.kafkastreams.integration.utils.IntegrationTestUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration test based on WordCountLambdaExample, using an embedded Kafka cluster.
 *
 * Note: This example uses lambda expressions and thus works with Java 8+ only.
 */
public class StreamingDroolsIntegrationTest {

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();
    private static final String inputTopic = "inputTopic";
    private static final String outputTopic = "outputTopic";
    private DroolsRulesApplier rulesApplier;


    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);
    }

    @Test
    public void testApplyRule() throws Exception {
        rulesApplier = new DroolsRulesApplier("IfContainsEPrepend0KS");
        List<String> inputValues = Arrays.asList("Hello", "Canal", "Camel");
        List<String> expectedOutput = Arrays.asList("0Hello", "Canal", "0Camel");

        Properties streamsConfiguration = createStreamConfig();

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<byte[], String> inputData = builder.stream(inputTopic);
        KStream<byte[], String> outputData = inputData.mapValues(rulesApplier::applyRule);

        outputData.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        Properties producerConfig = createProducerConfig();
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

        Properties consumerConfig = createConsumerConfig();
        List<String> actualOutput = IntegrationTestUtils
                .waitUntilMinValuesRecordsReceived(consumerConfig, outputTopic, expectedOutput.size());
        streams.close();
        assertThat(actualOutput).containsExactlyElementsOf(expectedOutput);
    }

    private Properties createStreamConfig() {
        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "drools-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zookeeperConnect());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return streamsConfiguration;
    }

    private Properties createProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return producerConfig;
    }

    private Properties createConsumerConfig() {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "integration-test-consumer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return consumerConfig;
    }
}