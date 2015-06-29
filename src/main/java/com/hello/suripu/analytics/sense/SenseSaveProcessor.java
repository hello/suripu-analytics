package com.hello.suripu.analytics.sense;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.codahale.metrics.annotation.Timed;
import java.util.List;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseSaveProcessor implements IRecordProcessor {

    public SenseSaveProcessor(){

    }

    public void initialize(String s) {

    }

    @Timed
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {
        System.out.println("Processing Records...");

    }

    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

    }
}
