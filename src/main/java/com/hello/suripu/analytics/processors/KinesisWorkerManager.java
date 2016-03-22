package com.hello.suripu.analytics.processors;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

public class KinesisWorkerManager implements Managed {

    private final static Logger LOGGER = LoggerFactory.getLogger(KinesisWorkerManager.class);

    private final ExecutorService executorService;
    private final Worker worker;

    public KinesisWorkerManager(final ExecutorService executorService, final Worker worker) {
        this.worker = worker;
        this.executorService = executorService;
    }


    @Override
    public void start() throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("worker={} action=starting", worker.getApplicationName());
                worker.run();
            }
        });
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("worker={} action=stopping", worker.getApplicationName());
        worker.shutdown();
        LOGGER.info("worker={} action=stopped", worker.getApplicationName());
    }
}
