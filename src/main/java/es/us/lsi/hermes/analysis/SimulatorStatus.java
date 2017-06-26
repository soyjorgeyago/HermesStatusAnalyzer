package es.us.lsi.hermes.analysis;

import es.us.lsi.hermes.util.Constants;
import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.StrNotNullOrEmpty;
import org.supercsv.cellprocessor.ift.CellProcessor;

import java.io.Serializable;
import java.util.Date;

/**
 * Clase con el estado de la simulación en cada segundo.
 */
public class SimulatorStatus implements Serializable {

    private String pcKey;
    private final long timestamp;
    private final String formattedDateTime;
    private final int generated;
    private final int sent;
    private final int ok;
    private final int notOk;
    private final int errors;
    private final int recovered;
    private final int pending;
    private final int runningThreads;
    private final long currentDriversDelay;
    private final int activeDrivers;
    private final int pausedDrivers;

    public SimulatorStatus() {
        this(null, System.currentTimeMillis(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public SimulatorStatus(String pcKey, long timestamp, int generated, int sent, int ok, int notOk, int errors, int recovered, int pending, int runningThreads, long currentDriversDelay, int activeDrivers, int pausedDrivers) {
        this.pcKey = pcKey;
        this.timestamp = timestamp;
        this.formattedDateTime = Constants.sdf.format(new Date(timestamp));
        this.generated = generated;
        this.sent = sent;
        this.ok = ok;
        this.notOk = notOk;
        this.errors = errors;
        this.recovered = recovered;
        this.pending = pending;
        this.runningThreads = runningThreads;
        this.currentDriversDelay = currentDriversDelay;
        this.activeDrivers = activeDrivers;
        this.pausedDrivers = pausedDrivers;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getFormattedDateTime() {
        return formattedDateTime;
    }
    
    public int getGenerated() {
        return generated;
    }

    public int getSent() {
        return sent;
    }

    public int getOk() {
        return ok;
    }

    public int getNotOk() {
        return notOk;
    }

    public int getErrors() {
        return errors;
    }

    public int getRecovered() {
        return recovered;
    }

    public int getPending() {
        return pending;
    }

    public int getRunningThreads() {
        return runningThreads;
    }

    public long getCurrentDriversDelay() {
        return currentDriversDelay;
    }
    
    public int getActiveDrivers() {
        return activeDrivers;
    }

    public int getPausedDrivers() {
        return pausedDrivers;
    }

    public String getPcKey() {
        return pcKey;
    }

    public void setPcKey(String pcKey) {
        this.pcKey = pcKey;
    }

    // ------------------------- CSV IMP/EXP -------------------------
    public final static CellProcessor[] cellProcessors = new CellProcessor[]{
        new StrNotNullOrEmpty(),
        new ParseLong(),
        new StrNotNullOrEmpty(),
        new ParseInt(),
        new ParseInt(),
        new ParseInt(),
        new ParseInt(),
        new ParseInt(),
        new ParseInt(),
        new ParseInt(),
        new ParseInt(),
        new ParseLong(),
        new ParseInt(),
        new ParseInt()};

    public final static String[] fields = new String[]{
        "pcKey",
        "timestamp",
        "formattedDateTime",
        "generated",
        "sent",
        "ok",
        "notOk",
        "errors",
        "recovered",
        "pending",
        "runningThreads",
        "currentDriversDelay",
        "activeDrivers",
        "pausedDrivers"};

    public final static String[] headers = new String[]{
        "PcKey",
        "Timestamp",
        "Time",
        "Generated",
        "Sent",
        "Ok",
        "NotOk",
        "Errors",
        "Recovered",
        "Pending",
        "RunningThreads",
        "CurrentDriversDelay",
        "ActiveDrivers",
        "PausedDrivers"};
}
