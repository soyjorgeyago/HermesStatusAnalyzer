package es.us.lsi.hermes;

import es.us.lsi.hermes.kafka.consumer.StatusAnalyserConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());
    private static final String PROPERTIES_FILENAME = "Kafka.properties";

    private static Properties kafkaProperties;

    public static void main(String[] args) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        
        try {
            InputStream input = classLoader.getResourceAsStream(PROPERTIES_FILENAME);
            kafkaProperties = new Properties();
            kafkaProperties.load(input);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "main() - Error loading properties file: " + PROPERTIES_FILENAME, ex);
            return;
        } catch (NullPointerException ex) {
            LOG.log(Level.SEVERE, "main() - File \'{0}\' not found", PROPERTIES_FILENAME);
            return;
        }

        long pollTimeout = Long.parseLong(kafkaProperties.getProperty("consumer.poll.timeout.ms", "5000"));
        LOG.log(Level.INFO, "main() - Polling data every {0} milliseconds", pollTimeout);

        StatusAnalyserConsumer statusAnalyserConsumer = new StatusAnalyserConsumer(pollTimeout);
        statusAnalyserConsumer.start();
    }

    public static Properties getKafkaProperties() {
        return kafkaProperties;
    }
}
