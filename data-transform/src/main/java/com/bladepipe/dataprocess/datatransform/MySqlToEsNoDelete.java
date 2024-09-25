package com.bladepipe.dataprocess.datatransform;

import java.util.ArrayList;
import java.util.List;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.EventTypeInSdk;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlToEsNoDelete implements CloudCanalProcessorV2 {

    @Override
    public void start(ProcessorContext context) {
    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getEventType() == EventTypeInSdk.DELETE) {
            CustomData newData = data.genUpdateFromDelete();
            re.add(newData);
        } else {
            re.add(data);
        }

        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
