package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.MD5Tool;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import org.apache.commons.math3.util.MathUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class O_FlatCount extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_FlatCount.class);

    public O_FlatCount() {
        super("FLAT_COUNT");
    }

    private static class FlatCountTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;

        public FlatCountTable(String name, long timestamp, double amount) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
        }
    }

    /**
     * "FLAT_COUNT(60d, transaction, SET(60d, transaction, phone, card_bin))"
     * <p>
     * FLAT_COUNT must has a SET target, it use the same target with SET.
     *
     * @param fql
     * @param event
     * @param helper
     * @param mode
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> doExecute(StreamFQL fql,
                                         JSONObject event,
                                         Map<String, Object> helper,
                                         String mode) throws Exception {
        logger.info(String.format("FeatureDSL[%s]", JSONObject.toJSONString(fql)));
        Map<String, Object> result = new HashMap<>();

        // on
        final SortedMap<String, Field> onFields = sortedOn(fql);

        // cache name
        String cacheName = genCacheName(fql, event);

        // time
        long atTime = ceilTimestamp(JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"),
                Long.class, System.currentTimeMillis()), fql.getWindow());
        long startTime = atTime - windowMilliSeconds(fql.getWindow());
        String windowSegmentId = getWindowSegmentId(atTime, fql.getWindow());

        if (isUpdateMode(mode)) {
            Object target = getValueFromEvent(event, fql.getTarget());
            if (isNotNull(target)) {
                if (!StreamFQLResult.class.isInstance(target)) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_COUNT invalid target[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(target),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }

                StreamFQLResult streamFQLResult = (StreamFQLResult) target;
                Field lastTarget = streamFQLResult.getFql().getTarget();
                Object targetValue = getValueFromEvent(event, lastTarget);
                if (targetValue != null) {
                    List<String> nameSplits = new ArrayList<>();
                    nameSplits.add(String.valueOf(targetValue));
                    for (Map.Entry<String, Field> on : onFields.entrySet()) {
                        nameSplits.add(getStringFromEvent(event, on.getValue()));
                    }
                    String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                    IgniteCache<String, FlatCountTable> cache = openIgniteCache(
                            cacheName, String.class, FlatCountTable.class, ttlSeconds(fql.getWindow()));

                    String id = String.format("%s_%s", MD5Tool.md5ID(name), windowSegmentId);
                    boolean succeed;
                    do {
                        FlatCountTable record = cache.get(id);
                        if (record == null) {
                            record = new FlatCountTable(name, atTime, 1);
                        } else {
                            record.amount += 1;
                        }
                        FlatCountTable old = cache.getAndPut(id, record);
                        succeed = (old == null) || (MathUtils.equals(old.amount, record.amount - 1));
                    } while (!succeed);
                }
            }
            result.put(VALUE_FIELD, Void.create());
        }

        if (isGetMode(mode)) {
            Object target = getValueFromConditionOrEvent(event, fql.getTarget());
            if (isNull(target)) {
                result.put(VALUE_FIELD, 0);
            } else {
                if (!StreamFQLResult.class.isInstance(target)) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_COUNT invalid target[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(target),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }

                StreamFQLResult streamFQLResult = (StreamFQLResult) target;
                Object lastResult = streamFQLResult.getResult();
                if (!Collection.class.isInstance(lastResult)) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_COUNT invalid lastResult[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(lastResult),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }

                List<String> names = new ArrayList<>();
                Collection targetValues = (Collection) lastResult;
                for (Object targetValue : targetValues) {
                    if (targetValue == null) {
                        continue;
                    }
                    List<String> nameSplits = new ArrayList<>();
                    nameSplits.add(String.valueOf(targetValue));
                    for (Map.Entry<String, Field> on : onFields.entrySet()) {
                        nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
                    }
                    String name = Joiner.on(SPLIT_SIGN).join(nameSplits);
                    names.add(name);
                }

                // sum query
                IgniteCache<String, FlatCountTable> cache = openIgniteCache(
                        cacheName, String.class, FlatCountTable.class, ttlSeconds(fql.getWindow()));
                SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                        "SELECT sum(amount) FROM FlatCountTable t1 join table(name varchar = ?) t2 " +
                                "ON t1.name = t2.name " +
                                "WHERE t1.timestamp > ? and t1.timestamp <= ?");
                List<List<?>> cursor = cache.query(sumQuery.setArgs(
                        names.toArray(new String[0]), startTime, atTime)).getAll();

                double sum = 0.0;
                for (List<?> row : cursor) {
                    if (row.get(0) != null) {
                        sum += (Double) row.get(0);
                    }
                }
                result.put(VALUE_FIELD, sum);
            }
        }

        return result;
    }
}
