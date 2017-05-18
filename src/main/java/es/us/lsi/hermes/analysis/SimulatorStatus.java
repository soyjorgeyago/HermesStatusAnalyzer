package es.us.lsi.hermes.analysis;

import org.supercsv.cellprocessor.ParseInt;
import org.supercsv.cellprocessor.ParseLong;
import org.supercsv.cellprocessor.constraint.StrNotNullOrEmpty;
import org.supercsv.cellprocessor.ift.CellProcessor;

import java.io.Serializable;

/**
 * Clase con el estado de la simulación en cada segundo.
 */
public class SimulatorStatus implements Serializable{

    private String pcKey;

    private final long timestamp;
    private final int generated;
    private final int sent;
    private final int ok;
    private final int notOk;
    private final int errors;
    private final int recovered;
    private final int pending;
    private final int runningThreads;
    private final long currentSmartDriversDelay;
    private final int pausedSmartDrivers;

    public SimulatorStatus() {
        this(System.currentTimeMillis(), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    public SimulatorStatus(long timestamp, int generated, int sent, int ok, int notOk, int errors, int recovered, int pending, int runningThreads, long currentSmartDriversDelay, int pausedSmartDrivers) {
        this.timestamp = timestamp;
        this.generated = generated;
        this.sent = sent;
        this.ok = ok;
        this.notOk = notOk;
        this.errors = errors;
        this.recovered = recovered;
        this.pending = pending;
        this.runningThreads = runningThreads;
        this.currentSmartDriversDelay = currentSmartDriversDelay;
        this.pausedSmartDrivers = pausedSmartDrivers;
    }

    public long getTimestamp() {
        return timestamp;
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

    public long getCurrentSmartDriversDelay() {
        return currentSmartDriversDelay;
    }

    public int getPausedSmartDrivers() {
        return pausedSmartDrivers;
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
            new ParseInt(),
            new ParseInt(),
            new ParseInt(),
            new ParseInt(),
            new ParseInt(),
            new ParseInt(),
            new ParseInt(),
            new ParseInt(),
            new ParseLong(),
            new ParseInt()};

    public final static String[] fields = new String[]{
            "pcKey",
            "timestamp",
            "generated",
            "sent",
            "ok",
            "notOk",
            "errors",
            "recovered",
            "pending",
            "runningThreads",
            "currentSmartDriversDelay",
            "pausedSmartDrivers"};

    public final static String[] headers = new String[]{
            "PcKey",
            "Timestamp",
            "Generated",
            "Sent",
            "Ok",
            "NotOk",
            "Errors",
            "Recovered",
            "Pending",
            "RunningThreads",
            "CurrentSmartDriversDelay",
            "PausedSmartDrivers"};
}