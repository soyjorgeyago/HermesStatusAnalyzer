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
    private ICsvBeanWriter beanWriter  = null;
    private int recordsByFile = 0;

    public StatusAnalyserConsumer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("StatusAnalyserConsumer", true);
        LOG.log(Level.INFO, "Initializing the Status Analyser - Consumer");
        this.consumer = new KafkaConsumer<>(Main.getKafkaProperties());
        this.pollTimeout = pollTimeout;
        this.gson = new Gson();

        // Add hook to close the writer
        Runtime.getRuntime().addShutdownHook(new Thread(this::closeBeanWriter));

        Utils.createCsvFolders(Constants.RECORDS_FOLDER);
        createBeanWriter();
    }


    @Override
    public void doWork() {
        //TODO Contemplar si se puede sacar el suscribe de aqui
        consumer.subscribe(Collections.singleton(Constants.TOPIC_SIMULATOR_STATUS));

        ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, String> record : records) {

            // Extract POJO from Json
            SimulatorStatus status = gson.fromJson(record.value(), SimulatorStatus.class);
            status.setPcKey(record.key());

            // Save POJO in CSV file
            try {
                beanWriter.write(status, SimulatorStatus.fields, SimulatorStatus.cellProcessors);
                recordsByFile++;
            }catch (IOException ex){
                LOG.log(Level.SEVERE, "doWork - Error storing the Status in the CSV");
            }

            // If the maximum number of records is archived, change files
            if(recordsByFile > Constants.MAX_RECORDS_BY_FILE) {
                createBeanWriter();
                recordsByFile = 0;
            }
        }
    }

    private void createBeanWriter(){
        if(beanWriter != null) {
            closeBeanWriter();
        }

        String file = Constants.RECORDS_FOLDER + Constants.RECORDS_HEADER + System.currentTimeMillis() + ".csv";

        try {
            beanWriter = new CsvBeanWriter(new FileWriter(file, true), CsvPreference.EXCEL_NORTH_EUROPE_PREFERENCE);
            beanWriter.writeHeader(SimulatorStatus.headers);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "exportToCSV() - Error exporting to CSV: " + file, ex);
        }
    }

    private void closeBeanWriter(){
        try {
            beanWriter.flush();
            beanWriter.close();
            LOG.log(Level.INFO, "closeBeanWriter() - 'beanWriter' closed");
        }catch (IOException ex){
            LOG.log(Level.SEVERE, "closeBeanWriter() - Error closing the 'writer'", ex);
        }
    }
}
