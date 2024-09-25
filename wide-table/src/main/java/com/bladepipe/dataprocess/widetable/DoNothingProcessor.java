package com.bladepipe.dataprocess.widetable;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class DoNothingProcessor implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        customLogger.info("Do nothing log");
        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
