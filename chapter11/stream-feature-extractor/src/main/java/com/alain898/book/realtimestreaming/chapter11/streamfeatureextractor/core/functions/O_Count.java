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

public class O_Count extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Count.class);

    public O_Count() {
        super("COUNT");
    }

    private static class CountTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;

        public CountTable(String name, long timestamp, double amount) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CountTable that = (CountTable) o;

            if (timestamp != that.timestamp) return false;
            if (Double.compare(that.amount, amount) != 0) return false;
            return name != null ? name.equals(that.name) : that.name == null;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = name != null ? name.hashCode() : 0;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            temp = Double.doubleToLongBits(amount);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
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
        long atTime = ceilTimestamp(JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"),
                Long.class, System.currentTimeMillis()), fql.getWindow());
        long startTime = atTime - windowMilliSeconds(fql.getWindow());
        String windowSegmentId = getWindowSegmentId(atTime, fql.getWindow());

        if (isUpdateMode(mode)) {
            String target = getStringFromEvent(event, fql.getTarget());
            if (isNotNull(target)) {
                List<String> nameSplits = new ArrayList<>();
                nameSplits.add(String.valueOf(target));
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, CountTable> cache = openIgniteCache(cacheName, String.class, CountTable.class,
                        ttlSeconds(fql.getWindow()));
                String id = String.format("%s_%s", MD5Tool.md5ID(name), windowSegmentId);

                boolean succeed;
                CountTable newRecord = new CountTable(name, atTime, 1);
                int retryTimes = RETRY_TIMES;
                do {
                    CountTable oldRecord = cache.get(id);
                    if (oldRecord != null) {
                        newRecord.amount = oldRecord.amount + 1;
                    } else {
                        oldRecord = newRecord;
                        cache.putIfAbsent(id, oldRecord);
                    }
                    succeed = cache.replace(id, oldRecord, newRecord);
                    retryTimes--;
                } while (!succeed && retryTimes > 0);
                if (!succeed) {
                    throw new IllegalStateException(String.format(
                            "COUNT failed to update index[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(name),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }

            }
            result.put(VALUE_FIELD, Void.create());
        }

        if (isGetMode(mode)) {
            String target = getStringFromConditionOrEvent(event, fql.getTarget());
            if (isNull(target)) {
                result.put(VALUE_FIELD, 0);
            } else {
                List<String> nameSplits = new ArrayList<>();
                nameSplits.add(String.valueOf(target));
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, CountTable> cache = openIgniteCache(cacheName, String.class, CountTable.class,
                        ttlSeconds(fql.getWindow()));
                // sum query
                SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                        "SELECT sum(amount) FROM CountTable " +
                                "WHERE name = ? and timestamp > ? and timestamp <= ?");
                List<List<?>> cursor = cache.query(sumQuery.setArgs(name, startTime, atTime)).getAll();
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
