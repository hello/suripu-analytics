package com.hello.suripu.analytics.sense;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

/**
 * Created by jnorgan on 6/29/15.
 */
public class SenseSaveProcessorFactory implements IRecordProcessorFactory {

    public SenseSaveProcessorFactory() {

    }

    public IRecordProcessor createProcessor() {
        return new SenseSaveProcessor();
    }
}
