package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.MD5Tool;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class O_FlatCountDistinct extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_FlatCountDistinct.class);

    public O_FlatCountDistinct() {
        super("FLAT_COUNT_DISTINCT");
    }

    private static class FlatCountDistinctTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private String value;

        public FlatCountDistinctTable(String name, long timestamp, String value) {
            this.name = name;
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    /**
     * "FLAT_COUNT_DISTINCT(60d, transaction, phone, SET(60d, transaction, card_no, userid), ip)"
     * <p>
     * FLAT_COUNT_DISTINCT must has a SET target, it use the same target with SET.
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
        Map<String, Object> result = new HashMap<>();

        // cache name
        String cacheName = genCacheName(fql, event);

        // time
        long atTime = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"), Long.class,
                System.currentTimeMillis());
        long startTime = atTime - windowMilliSeconds(fql.getWindow());

        if (isUpdateMode(mode)) {
            String target = getStringFromEvent(event, fql.getTarget());
            if (isNotNull(target)) {
                if (CollectionUtils.isEmpty(fql.getOn())) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_COUNT_DISTINCT invalid on[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(fql.getOn()),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }

                Object firstOnResult = getValueFromEvent(event, fql.getOn().get(0));
                if (!StreamFQLResult.class.isInstance(firstOnResult)) {
                    throw new IllegalArgumentException(String.format(
                            "FLAT_COUNT_DISTINCT invalid firstOnResult[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(firstOnResult),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }
                StreamFQLResult firstOnStreamFQLResult = (StreamFQLResult) firstOnResult;

                final SortedMap<String, Field> onFields = Maps.newTreeMap();
                List<Field> onList = new ArrayList<>(fql.getOn());
                onList.set(0, firstOnStreamFQLResult.getFql().getTarget());
                onList.forEach(x -> onFields.put(x.getField(), x));

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, FlatCountDistinctTable> cache = openIgniteCache(
                        cacheName, String.class, FlatCountDistinctTable.class, ttlSeconds(fql.getWindow()));

                String id = MD5Tool.md5ID(String.format("%s_%s", name, target));
                FlatCountDistinctTable record = cache.get(id);
                if (record == null) {
                    record = new FlatCountDistinctTable(name, atTime, target);
                } else {
                    record.timestamp = atTime;
                }
                cache.put(id, record);
            }

            result.put(VALUE_FIELD, Void.create());
        }

        if (isGetMode(mode)) {
            if (CollectionUtils.isEmpty(fql.getOn())) {
                throw new IllegalArgumentException(String.format(
                        "FLAT_COUNT_DISTINCT invalid on[%s], featureDSL[%s], event[%s]",
                        JSONObject.toJSONString(fql.getOn()),
                        JSONObject.toJSONString(fql),
                        JSONObject.toJSONString(event)));
            }

            Object firstOnResult = getValueFromEvent(event, fql.getOn().get(0));
            if (!StreamFQLResult.class.isInstance(firstOnResult)) {
                throw new IllegalArgumentException(String.format(
                        "FLAT_COUNT_DISTINCT invalid firstOnResult[%s], featureDSL[%s], event[%s]",
                        JSONObject.toJSONString(firstOnResult),
                        JSONObject.toJSONString(fql),
                        JSONObject.toJSONString(event)));
            }
            StreamFQLResult firstOnStreamFQLResult = (StreamFQLResult) firstOnResult;

            Object firstOnComplexResultValue = firstOnStreamFQLResult.getResult();
            if (!Collection.class.isInstance(firstOnComplexResultValue)) {
                throw new IllegalArgumentException(String.format(
                        "FLAT_COUNT_DISTINCT invalid firstOnComplexResultValue[%s], featureDSL[%s], event[%s]",
                        JSONObject.toJSONString(firstOnComplexResultValue),
                        JSONObject.toJSONString(fql),
                        JSONObject.toJSONString(event)));
            }

            final SortedMap<String, Field> onFields = Maps.newTreeMap();
            List<Field> onList = new ArrayList<>(fql.getOn());
            onList.set(0, firstOnStreamFQLResult.getFql().getTarget());
            onList.forEach(x -> onFields.put(x.getField(), x));

            List<String> names = new ArrayList<>();
            Collection firstOnValues = (Collection) firstOnComplexResultValue;
            for (Object firstOnValue : firstOnValues) {
                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    if (on.getKey().equals(firstOnStreamFQLResult.getFql().getTarget().getField())) {
                        nameSplits.add(String.valueOf(firstOnValue));
                    } else {
                        nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
                    }
                }
                names.add(Joiner.on(SPLIT_SIGN).join(nameSplits));
            }

            // sum query
            IgniteCache<String, FlatCountDistinctTable> cache = openIgniteCache(
                    cacheName, String.class, FlatCountDistinctTable.class, ttlSeconds(fql.getWindow()));
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT count(DISTINCT value) FROM FlatCountDistinctTable t1 join table(name varchar = ?) t2 " +
                            "ON t1.name = t2.name " +
                            "WHERE t1.timestamp > ? and t1.timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(
                    names.toArray(new String[0]), startTime, atTime)).getAll();

            long sum = 0L;
            for (List<?> row : cursor) {
                if (row.get(0) != null) {
                    sum += (Long) row.get(0);
                }
            }
            result.put(VALUE_FIELD, sum);
        }

        return result;
    }
}
