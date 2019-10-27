package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.MD5Tool;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

public class O_Set extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Set.class);

    public O_Set() {
        super("SET");
    }

    private static class SetTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private String value;

        public SetTable(String name, long timestamp, String value) {
            this.name = name;
            this.timestamp = timestamp;
            this.value = value;
        }
    }

    @Override
    public Map<String, Object> doExecute(StreamFQL fql,
                                         JSONObject event,
                                         Map<String, Object> helper,
                                         String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();

        // on
        final SortedMap<String, Field> onFields = sortedOn(fql);

        // cache name
        String cacheName = genCacheName(fql, event);

        // time
        long atTime = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"), Long.class,
                System.currentTimeMillis());
        long startTime = atTime - windowMilliSeconds(fql.getWindow());

        if (isUpdateMode(mode)) {
            String target = getStringFromEvent(event, fql.getTarget());
            if (isNotNull(target)) {
                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, SetTable> cache = openIgniteCache(cacheName, String.class, SetTable.class,
                        ttlSeconds(fql.getWindow()));

                String id = MD5Tool.md5ID(String.format("%s_%s", name, target));
                SetTable record = cache.get(id);
                if (record == null) {
                    record = new SetTable(name, atTime, target);
                } else {
                    record.timestamp = atTime;
                }
                cache.put(id, record);
            }

            StreamFQLResult streamFQLResult = new StreamFQLResult();
            streamFQLResult.setFql(fql);
            streamFQLResult.setResult(Void.create());
            result.put(VALUE_FIELD, streamFQLResult);
        }

        if (isGetMode(mode)) {
            List<String> nameSplits = new ArrayList<>();
            for (Map.Entry<String, Field> on : onFields.entrySet()) {
                nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
            }
            String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

            IgniteCache<String, SetTable> cache = openIgniteCache(cacheName, String.class, SetTable.class,
                    ttlSeconds(fql.getWindow()));
            // sum query
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT value FROM SetTable " +
                            "WHERE name = ? and timestamp > ? and timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(name, startTime, atTime)).getAll();
            Set<String> set = new HashSet<>();
            for (List<?> row : cursor) {
                if (row.get(0) != null) {
                    set.add(String.valueOf(row.get(0)));
                }
            }

            StreamFQLResult streamFQLResult = new StreamFQLResult();
            streamFQLResult.setFql(fql);
            streamFQLResult.setResult(set);
            result.put(VALUE_FIELD, streamFQLResult);
        }

        return result;
    }

    @Override
    protected Map<String, Object> defaultValue(StreamFQL fql, JSONObject event,
                                               Map<String, Object> helper, String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();
        StreamFQLResult streamFQLResult = new StreamFQLResult();
        streamFQLResult.setFql(fql);
        streamFQLResult.setResult(Void.create());
        result.put(VALUE_FIELD, streamFQLResult);
        return result;
    }
}
