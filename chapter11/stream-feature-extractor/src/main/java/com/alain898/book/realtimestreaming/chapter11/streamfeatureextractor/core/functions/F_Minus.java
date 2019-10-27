package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class F_Minus extends AbstractFunction {

    public F_Minus() {
        super("F_MINUS");
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

        double on1Value = getDoubleFromConditionOrEvent(event, onFields.get(0));
        double on2Value = getDoubleFromConditionOrEvent(event, onFields.get(1));
        double value = on1Value - on2Value;

        Map<String, Object> result = new HashMap<>();
        result.put(VALUE_FIELD, value);
        return result;
    }
}
