package uk.co.hadoopathome.kafkastreams;

import org.apache.commons.configuration2.PropertiesConfiguration;
import uk.co.hadoopathome.kafkastreams.configuration.ConfigurationReader;

public class KafkaStreamsDroolsMain {

    public static void main(String[] args) {
        PropertiesConfiguration properties = ConfigurationReader.getProperties("config.properties");
        KafkaStreamsRunner.runKafkaStream(properties);
    }
}
