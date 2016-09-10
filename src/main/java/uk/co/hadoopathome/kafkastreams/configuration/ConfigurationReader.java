package uk.co.hadoopathome.kafkastreams.configuration;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;

/**
 * Reads configuration from a configuration file.
 */
public class ConfigurationReader {

    /**
     * Reads the configuration file.
     *
     * @param configurationFile the name of the configuration file in relation to the classpath
     * @return the populated PropertiesConfiguration object
     */
    public static PropertiesConfiguration getProperties(String configurationFile) {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
                new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class)
                        .configure(new Parameters().properties()
                                .setFileName(configurationFile)
                                .setThrowExceptionOnMissing(true));
        try {
            return builder.getConfiguration();
        } catch (ConfigurationException e) {
            throw new RuntimeException("Unable to parse configuration. Exiting");
        }
    }
}
