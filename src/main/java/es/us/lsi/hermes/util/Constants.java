package es.us.lsi.hermes.util;

import java.text.SimpleDateFormat;

public final class Constants {

    public static final String TOPIC_SIMULATOR_STATUS = "SimulatorStatus";
    public static final String RECORDS_FOLDER = "statuses_csv";
    public static final long MAX_RECORDS_BY_FILE = 10000;
    public static final SimpleDateFormat dfFile = new SimpleDateFormat("yyyy-MM-dd_HH.mm.ss");
        public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Constants() {
    }
}
