package com.bladepipe.dataprocess.widetable;

import java.util.*;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;
import com.clougence.cloudcanal.sdk.api.modelv2.SchemaInfo;

/**
 * only log data
 *
 * @author bucketli 2021/11/29 23:07:26
 */
public class RecordLogger implements CloudCanalProcessorV2 {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        try {
            if (data.getRecords() == null) {
                return Collections.singletonList(data);
            }

            String fullTabName = getFullTableName(data.getSchemaInfo());

            for (CustomRecordV2 r : data.getRecords()) {
                LinkedHashMap<String, CustomFieldV2> keyData = null;
                switch (data.getEventType()) {
                    case INSERT:
                        keyData = r.getAfterKeyColumnMap();
                        break;
                    case UPDATE:
                    case DELETE:
                        keyData = r.getBeforeKeyColumnMap();
                        break;
                    default:
                        break;
                }

                if (keyData == null) {
                    continue;
                }

                StringBuilder sb = new StringBuilder(fullTabName);
                sb.append(",key data:[");
                boolean first = true;
                for (Map.Entry<String, CustomFieldV2> entry : keyData.entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        sb.append(",");
                    }

                    sb.append(entry.getValue().getFieldName()).append("=").append(entry.getValue().getValue());
                }

                sb.append("]");

                customLogger.info(sb.toString());
            }
        } catch (Exception e) {
            customLogger.error("in custom code,log data error.msg:" + ExceptionUtils.getRootCauseMessage(e), e);
        }

        List<CustomData> re = new ArrayList<>();
        re.add(data);
        return re;
    }

    protected String getFullTableName(SchemaInfo schemaInfo) {
        if (schemaInfo.getTable() != null) {
            StringBuilder sb = new StringBuilder();
            if (schemaInfo.getCatalog() != null) {
                sb.append(schemaInfo.getCatalog()).append(".");
            }

            if (schemaInfo.getSchema() != null) {
                sb.append(schemaInfo.getSchema()).append(".");
            }

            sb.append(schemaInfo.getTable());
            return sb.toString();
        } else if (schemaInfo.getTopic() != null) {
            return schemaInfo.getTopic();
        } else {
            return "Unknow table";
        }
    }

    @Override
    public void stop() {
        // do nothing
    }
}
