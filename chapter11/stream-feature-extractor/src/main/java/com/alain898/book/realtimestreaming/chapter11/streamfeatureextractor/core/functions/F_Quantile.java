package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.DynamicHistogram;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class F_Quantile extends AbstractFunction {
    public F_Quantile() {
        super("F_QUANTILE");
    }

    @Override
    protected Map<String, Object> doExecute(StreamFQL fql,
                                            JSONObject event,
                                            Map<String, Object> helper,
                                            String mode) throws Exception {

        final List<Field> onFields = fql.getOn();
        if (onFields.size() != 2) {
            throw new IllegalArgumentException(String.format(
                    "op[%s] invalid on[%s], featureDSL[%s], event[%s]",
                    fql.getOp(),
                    JSONObject.toJSONString(fql.getOn()),
                    JSONObject.toJSONString(fql),
                    JSONObject.toJSONString(event)));
        }

        Object complexResultObj = getValueFromEvent(event, onFields.get(0));

        if (!StreamFQLResult.class.isInstance(complexResultObj)) {
            throw new IllegalArgumentException(String.format(
                    "op[%s] invalid histogramObj[%s], featureDSL[%s], event[%s]",
                    fql.getOp(),
                    JSONObject.toJSONString(complexResultObj),
                    JSONObject.toJSONString(fql),
                    JSONObject.toJSONString(event)));
        }

        StreamFQLResult streamFQLResult = (StreamFQLResult) complexResultObj;
        Object histogramObj = streamFQLResult.getResult();
        if (!List.class.isInstance(histogramObj)) {
            throw new IllegalArgumentException(String.format(
                    "op[%s] invalid lastResult[%s], featureDSL[%s], event[%s]",
                    fql.getOp(),
                    JSONObject.toJSONString(histogramObj),
                    JSONObject.toJSONString(fql),
                    JSONObject.toJSONString(event)));
        }

        Integer precision = getIntFromConditionOrEvent(event, onFields.get(1));
        if (precision == 0) {
            throw new IllegalArgumentException(String.format(
                    "op[%s] invalid precision[%s], featureDSL[%s], event[%s]",
                    fql.getOp(),
                    JSONObject.toJSONString(precision),
                    JSONObject.toJSONString(fql),
                    JSONObject.toJSONString(event)));
        }

        List<DynamicHistogram.Bin> bins = (List<DynamicHistogram.Bin>) histogramObj;
        DynamicHistogram.Quantile quantile = DynamicHistogram.quantile(bins, precision);
        Map<String, Object> result = new HashMap<>();
        StreamFQLResult quantileResult = new StreamFQLResult();
        quantileResult.setFql(fql);
        quantileResult.setResult(JSON.toJSON(quantile));
        result.put(VALUE_FIELD, quantileResult);
        return result;
    }
}
