package com.bladepipe.dataprocess.datatransform;

import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * only log data
 *
 * @author bucketli 2021/11/29 23:07:26
 */
public class OracleSrcRecordLogger implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    private DataSource            srcDs;

    @Override
    public void start(ProcessorContext context) {
        srcDs = (DataSource) context.getSrcRdbDs();
    }

    @Override
    public List<CustomData> process(CustomData data) {
        try {
            String dataJson = new ObjectMapper().writeValueAsString(data);
            customLogger.info("lol. " + dataJson);
        } catch (Exception e) {
            customLogger.error("lol,log data error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }

        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
