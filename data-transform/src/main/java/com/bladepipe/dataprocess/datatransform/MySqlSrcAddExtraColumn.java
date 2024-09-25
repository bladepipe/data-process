package com.bladepipe.dataprocess.datatransform;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.clougence.cloudcanal.sdk.api.CloudCanalProcessorV2;
import com.clougence.cloudcanal.sdk.api.ProcessorContext;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomData;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomFieldV2;
import com.clougence.cloudcanal.sdk.api.modelv2.CustomRecordV2;

/**
 * @author bucketli 2021/11/29 23:07:26
 */
public class MySqlSrcAddExtraColumn implements CloudCanalProcessorV2 {

    @Override
    public void start(ProcessorContext context) {

    }

    @Override
    public List<CustomData> process(CustomData data) {
        List<CustomData> re = new ArrayList<>();
        if (data.getSchemaInfo().getSchema().equals("user_db") && data.getSchemaInfo().getTable().equals("product")) {
            for (CustomRecordV2 recordV2 : data.getRecords()) {
                recordV2.addField(CustomFieldV2.buildField("extra_col_bigint", 123, Types.BIGINT, "bigint(20)", false, false, true));
                recordV2.addField(CustomFieldV2.buildField("extra_col_varchar", "added_str", Types.VARCHAR, "varchar(255)", false, false, true));
                recordV2.addField(CustomFieldV2.buildField("day", new Date(), Types.DATE, "date", false, false, true));
            }
        }

        re.add(data);
        return re;
    }

    @Override
    public void stop() {
        // do nothing
    }
}
