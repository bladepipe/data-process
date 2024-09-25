package com.bladepipe.dataprocess.datatransform;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessor;
import com.clougence.cloudcanal.sdk.api.constant.rdb.RecordAction;
import com.clougence.cloudcanal.sdk.api.contextkey.RdbContextKey;
import com.clougence.cloudcanal.sdk.api.metakey.RdbMetaKey;
import com.clougence.cloudcanal.sdk.api.model.CustomField;
import com.clougence.cloudcanal.sdk.api.model.CustomProcessorContext;
import com.clougence.cloudcanal.sdk.api.model.CustomRecord;

/**
 * mysql sync to clickHouse to prevent duplicate data modification
 *
 * @author yisai
 * @Date 2021-12-23 17:23
 */
public class MysqlToClickHouseUpdateNoRepeat implements CloudCanalProcessor {

    protected static final Logger customLogger = LoggerFactory.getLogger("custom_processor");

    private String                schemaName;

    private String                tableName;

    private Long                  primaryKey;

    private String                actionName;

    @Override
    public List<CustomRecord> process(List<CustomRecord> customRecordList, CustomProcessorContext customProcessorContext) {
        // Delete old data from the peer database before modifying the synchronization
        updateRecord(customRecordList, (DataSource) customProcessorContext.getProcessorContextMap().get(RdbContextKey.TARGET_DATASOURCE));
        return customRecordList;
    }

    /**
     * Before modifying the synchronization, delete the old data of the peer database. Ensure that the primary key is placed in the first field of the table
     *
     * @author yisai
     * @Date 2021-12-23 17:00
     */
    private void updateRecord(List<CustomRecord> customRecordList, DataSource dataSource) {
        try {
            Connection connection = dataSource.getConnection();
            for (CustomRecord customRecord : customRecordList) {
                actionName = customRecord.getRecordMetaMap().get(RdbMetaKey.ACTION_NAME).toString();
                if (RecordAction.UPDATE.name().equals(actionName)) {
                    schemaName = customRecord.getRecordMetaMap().get(RdbMetaKey.SCHEMA_NAME).toString();
                    tableName = customRecord.getRecordMetaMap().get(RdbMetaKey.TABLE_NAME).toString();
                    int endIndex = customRecord.getFieldMapBefore().toString().indexOf("=");
                    String primaryKeyName = customRecord.getFieldMapBefore().toString().substring(1, endIndex);
                    CustomField primaryKeyCustomField = customRecord.getFieldMapBefore().get(primaryKeyName);
                    primaryKey = Long.parseLong(primaryKeyCustomField.getValue().toString());
                    PreparedStatement ps = connection.prepareStatement("ALTER TABLE " + schemaName + "." + tableName + " DELETE WHERE " + primaryKeyName + " = " + primaryKey);
                    ps.execute();
                }
            }
        } catch (Exception e) {
            customLogger.error(ExceptionUtils.getRootCauseMessage(e), e);
        }
    }
}
