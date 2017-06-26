package es.us.lsi.hermes.kafka.consumer;

import com.google.gson.Gson;
import es.us.lsi.hermes.Main;
import es.us.lsi.hermes.analysis.SimulatorStatus;
import es.us.lsi.hermes.util.Constants;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import es.us.lsi.hermes.util.Utils;
import java.io.File;
import java.util.Date;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.supercsv.io.CsvBeanWriter;
import org.supercsv.io.ICsvBeanWriter;
import org.supercsv.prefs.CsvPreference;

public class StatusAnalyserConsumer extends ShutdownableThread {

    private static final Logger LOG = Logger.getLogger(StatusAnalyserConsumer.class.getName());

    private final KafkaConsumer<String, String> consumer;
    private final long pollTimeout;
    private final Gson gson;
    private ICsvBeanWriter beanWriter = null;
    private int recordsByFile = 0;

    public StatusAnalyserConsumer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("StatusAnalyserConsumer", true);
        LOG.log(Level.INFO, "StatusAnalyserConsumer() - Initializing the status analyser consumer");
        this.consumer = new KafkaConsumer<>(Main.getKafkaProperties());
        this.pollTimeout = pollTimeout;
        this.gson = new Gson();
        consumer.subscribe(Collections.singleton(Constants.TOPIC_SIMULATOR_STATUS));

        Utils.createCsvFolders(Constants.RECORDS_FOLDER);
        createBeanWriter();
    }

    @Override
    public void doWork() {
        ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, String> record : records) {

            // Extract POJO from Json.
            SimulatorStatus status = gson.fromJson(record.value(), SimulatorStatus.class);
            status.setPcKey(record.key());

            // If the maximum number of records is archived, close the current file and continue in a new file.
            if (recordsByFile > Constants.MAX_RECORDS_BY_FILE) {
                closeBeanWriter();
                createBeanWriter();
                recordsByFile = 0;
            }

            try {
                beanWriter.write(status, SimulatorStatus.fields, SimulatorStatus.cellProcessors);
                recordsByFile++;
                beanWriter.flush();
            } catch (Exception ex) {
                LOG.log(Level.SEVERE, "doWork() - Error storing the status in the CSV", ex);
            }
        }
    }

    private void createBeanWriter() {
        if (beanWriter != null) {
            closeBeanWriter();
        }

        String filePath = Constants.RECORDS_FOLDER + File.separator + Constants.dfFile.format(new Date()) + ".csv";

        try {
            beanWriter = new CsvBeanWriter(new FileWriter(filePath, true), CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
            beanWriter.writeHeader(SimulatorStatus.headers);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "createBeanWriter() - Error creating the CSV file: " + filePath, ex);
        }
    }

    private void closeBeanWriter() {
        try {
            beanWriter.flush();
            beanWriter.close();
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "closeBeanWriter() - Error closing the CSV file", ex);
        } finally {
            beanWriter = null;
        }
    }

    @Override
    public void shutdown() {
        super.shutdown();
        closeBeanWriter();
        shutdown();
    }

}
