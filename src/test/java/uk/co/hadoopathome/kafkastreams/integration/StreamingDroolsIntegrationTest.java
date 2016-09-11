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

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.junit.Test;
import uk.co.hadoopathome.kafkastreams.KafkaStreamsRunner;
import uk.co.hadoopathome.kafkastreams.configuration.ConfigurationReader;
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

    @Test
    public void testApplication() throws Exception {
        PropertiesConfiguration properties = ConfigurationReader.getProperties("config.properties");
        properties.setProperty("bootstrapServers", CLUSTER.bootstrapServers());
        properties.setProperty("zookeeperServers", CLUSTER.zookeeperConnect());

        String inputTopic = (String) properties.getProperty("inputTopic");
        String outputTopic = (String) properties.getProperty("outputTopic");
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);

        List<String> inputValues = Arrays.asList("Hello", "Canal", "Camel");
        List<String> expectedOutput = Arrays.asList("0Hello", "Canal", "0Camel");
        String statePath = (String) properties.getProperty(StreamsConfig.STATE_DIR_CONFIG);
        IntegrationTestUtils.purgeLocalStreamsState(statePath);

        KafkaStreams streams = KafkaStreamsRunner.runKafkaStream(properties);

        Properties producerConfig = createProducerConfig();
        IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues, producerConfig);

        Properties consumerConfig = createConsumerConfig();
        List<String> actualOutput = IntegrationTestUtils
                .waitUntilMinValuesRecordsReceived(consumerConfig, outputTopic, expectedOutput.size());
        assertThat(actualOutput).containsExactlyElementsOf(expectedOutput);
        streams.close();
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