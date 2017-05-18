package es.us.lsi.hermes;

import es.us.lsi.hermes.kafka.consumer.StatusAnalyserConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    // Valores para la configuración del 'consumer' de Kafka.
    private static Properties kafkaProperties;

    public static void main(String[] args) {
        LOG.log(Level.INFO, "Setting up Kafka");

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        try {
            InputStream input = classLoader.getResourceAsStream("Kafka.properties");
            kafkaProperties = new Properties();
            kafkaProperties.load(input);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "main() - Problems loading Kafka's property files", ex);
            return;
        }

        long pollTimeout = Long.parseLong(kafkaProperties.getProperty("consumer.poll.timeout.ms", "5000"));
        LOG.log(Level.INFO, "Polling data every {0} milliseconds", pollTimeout);

        StatusAnalyserConsumer statusAnalyserConsumer = new StatusAnalyserConsumer(pollTimeout);
        statusAnalyserConsumer.start();
    }

    public static Properties getKafkaProperties() {
        return kafkaProperties;
    }
}
