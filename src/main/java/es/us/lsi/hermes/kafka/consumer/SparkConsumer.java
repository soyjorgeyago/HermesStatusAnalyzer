package es.us.lsi.hermes.kafka.consumer;

import com.google.gson.Gson;
import es.us.lsi.hermes.Main;
import es.us.lsi.hermes.util.Constants;
import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.supercsv.io.ICsvBeanWriter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkConsumer extends ShutdownableThread {

    private static final Logger LOG = Logger.getLogger(SparkConsumer.class.getName());

    private final KafkaConsumer<String, String> consumer;
    private final long pollTimeout;
    private final Gson gson;
    private ICsvBeanWriter beanWriter = null;
    private int recordsByFile = 0;

    public SparkConsumer(long pollTimeout) {
        // Podrá ser interrumpible.
        super("StatusAnalyserConsumer", true);
        LOG.log(Level.INFO, "StatusAnalyserConsumer() - Initializing the status analyser consumer");
        this.consumer = new KafkaConsumer<>(Main.getKafkaProperties());
        this.pollTimeout = pollTimeout;
        this.gson = new Gson();

//        Utils.createCsvFolders(Constants.RECORDS_FOLDER);
        createBeanWriter();
    }

    @Override
    public void doWork() {
        System.out.println("Ejecuta - Entra");
        try {
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", Main.getKafkaProperties().getProperty("bootstrap.servers"));
            kafkaParams.put("key.deserializer", LongDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", Main.getKafkaProperties().getProperty("group.id"));
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);

            Collection<String> topics = Collections.singleton(Constants.TOPIC_SIMULATOR_STATUS);

            //FIXME
            System.out.println("SI");
            SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkConsumer");
                                                    // execution threads
                                                                            // app name
            //FIXME
            System.out.println("NO");

            JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));
            // batch size

            final JavaInputDStream<ConsumerRecord<String, String>> stream =
                    KafkaUtils.createDirectStream(
                            streamingContext,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                    );


            stream.print();

            // Start the computation
            streamingContext.start();
            streamingContext.awaitTermination();

//            stream.mapToPair(
//                    new PairFunction<ConsumerRecord<String, String>, String, String>() {
//                        @Override
//                        public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
//                            return new Tuple2<>(record.key(), record.value());
//                        }
//                    });
//
//            stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
//                @Override
//                public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
//                    OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//
//                    System.out.println("SPARK llegan");
//                    System.out.println(rdd.rdd().toString());
////                Object results = yourCalculation(rdd);
//
//                    // begin your transaction
//
//                    // update results
//                    // update offsets where the end of existing offsets matches the beginning of this batch of offsets
//                    // assert that offsets were updated correctly
//
//                    // end your transaction
//                }
//            });
        } catch (Exception e){
            System.out.println("Exception");
        }

        // TODO: Study extracting the subscription from here.
        consumer.subscribe(Collections.singleton(Constants.TOPIC_SIMULATOR_STATUS));

        ConsumerRecords<String, String> records = consumer.poll(pollTimeout);
        for (ConsumerRecord<String, String> record : records) {
            System.out.println("KAFKA llegan");
        }


        System.out.println("Ejecuta - Sale");
    }

    private void createBeanWriter() {
    }

    private void closeBeanWriter() {
    }

    @Override
    public void shutdown() {
        super.shutdown();
        closeBeanWriter();
        shutdown();
    }

}
