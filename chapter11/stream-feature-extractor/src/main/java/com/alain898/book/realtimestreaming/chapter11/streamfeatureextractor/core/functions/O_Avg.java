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
import java.math.BigDecimal;
import java.util.*;

public class O_Avg extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Avg.class);

    public O_Avg() {
        super("AVG");
    }


    private static class AvgTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;
        @QuerySqlField
        private double n;

        public AvgTable(String name, long timestamp, double amount, double n) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
            this.n = n;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AvgTable avgTable = (AvgTable) o;

            if (timestamp != avgTable.timestamp) return false;
            if (Double.compare(avgTable.amount, amount) != 0) return false;
            if (Double.compare(avgTable.n, n) != 0) return false;
            return name != null ? name.equals(avgTable.name) : avgTable.name == null;
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = name != null ? name.hashCode() : 0;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            temp = Double.doubleToLongBits(amount);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(n);
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
            if (isValidNumber(target)) {
                double amount = new BigDecimal(target).doubleValue();

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, AvgTable> cache = openIgniteCache(cacheName, String.class, AvgTable.class,
                        ttlSeconds(fql.getWindow()));

                String id = String.format("%s_%s", MD5Tool.md5ID(name), windowSegmentId);
                AvgTable newRecord = new AvgTable(name, atTime, amount, 1);
                int retryTimes = RETRY_TIMES;
                boolean succeed;
                do {
                    AvgTable oldRecord = cache.get(id);
                    if (oldRecord != null) {
                        newRecord.n = oldRecord.n + 1;
                        newRecord.amount = oldRecord.amount + amount;
                    } else {
                        oldRecord = newRecord;
                        cache.putIfAbsent(id, oldRecord);
                    }
                    succeed = cache.replace(id, oldRecord, newRecord);
                    retryTimes--;
                } while (!succeed && retryTimes > 0);
                if (!succeed) {
                    throw new IllegalStateException(String.format(
                            "AVG failed to update index[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(name),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }
            }

            result.put(VALUE_FIELD, Void.create());
        }

        if (isGetMode(mode)) {
            List<String> nameSplits = new ArrayList<>();
            for (Map.Entry<String, Field> on : onFields.entrySet()) {
                nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
            }
            String name = Joiner.on(SPLIT_SIGN).join(nameSplits);
            IgniteCache<String, AvgTable> cache = openIgniteCache(cacheName, String.class, AvgTable.class,
                    ttlSeconds(fql.getWindow()));
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT amount, n FROM AvgTable " +
                            "WHERE name = ? and timestamp > ? and timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(name, startTime, atTime)).getAll();
            double amountSum = 0.0;
            double nSum = 0.0;
            for (List<?> row : cursor) {
                if (row.get(0) != null && row.get(1) != null) {
                    amountSum += (Double) row.get(0);
                    nSum += (Double) row.get(1);
                }
            }
            double avg;
            if (MathUtils.equals(nSum, 0)) {
                avg = Double.NaN;
            } else {
                avg = amountSum / nSum;
            }
            result.put(VALUE_FIELD, avg);
        }
        return result;
    }

}
