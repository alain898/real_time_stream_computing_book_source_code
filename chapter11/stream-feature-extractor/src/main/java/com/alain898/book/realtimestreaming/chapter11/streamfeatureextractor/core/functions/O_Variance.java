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

public class O_Variance extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Variance.class);

    public O_Variance() {
        super("VARIANCE");
    }


    private static class VarianceTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;
        @QuerySqlField
        private double amount2;
        @QuerySqlField
        private double n;

        public VarianceTable(String name, long timestamp, double amount, double amount2, double n) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
            this.amount2 = amount2;
            this.n = n;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VarianceTable that = (VarianceTable) o;

            if (timestamp != that.timestamp) return false;
            if (Double.compare(that.amount, amount) != 0) return false;
            if (Double.compare(that.amount2, amount2) != 0) return false;
            if (Double.compare(that.n, n) != 0) return false;
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
            temp = Double.doubleToLongBits(amount2);
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

                IgniteCache<String, VarianceTable> cache = openIgniteCache(cacheName, String.class, VarianceTable.class,
                        ttlSeconds(fql.getWindow()));

                String id = String.format("%s_%s", MD5Tool.md5ID(name), windowSegmentId);
                VarianceTable newRecord = new VarianceTable(name, atTime, amount, Math.pow(amount, 2), 1);
                int retryTimes = RETRY_TIMES;
                boolean succeed;
                do {
                    VarianceTable oldRecord = cache.get(id);
                    if (oldRecord != null) {
                        newRecord.n = oldRecord.n + 1;
                        newRecord.amount = oldRecord.amount + amount;
                        newRecord.amount2 = oldRecord.amount2 + Math.pow(amount, 2);
                    } else {
                        oldRecord = newRecord;
                        cache.putIfAbsent(id, oldRecord);
                    }
                    succeed = cache.replace(id, oldRecord, newRecord);
                    retryTimes--;
                } while (!succeed && retryTimes > 0);
                if (!succeed) {
                    throw new IllegalStateException(String.format(
                            "VARIANCE failed to update index[%s], featureDSL[%s], event[%s]",
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
            IgniteCache<String, VarianceTable> cache = openIgniteCache(cacheName, String.class, VarianceTable.class,
                    ttlSeconds(fql.getWindow()));
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT amount, amount2, n FROM VarianceTable " +
                            "WHERE name = ? and timestamp > ? and timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(name, startTime, atTime)).getAll();
            double amountSum = 0.0;
            double amount2Sum = 0.0;
            double nSum = 0.0;
            for (List<?> row : cursor) {
                if (row.get(0) != null && row.get(1) != null && row.get(2) != null) {
                    amountSum += (Double) row.get(0);
                    amount2Sum += (Double) row.get(1);
                    nSum += (Double) row.get(2);
                }
            }
            double avg;
            double avg2;
            if (MathUtils.equals(nSum, 0)) {
                avg = Double.NaN;
                avg2 = Double.NaN;
            } else {
                avg = amountSum / nSum;
                avg2 = amount2Sum / nSum;
            }
            // E[X-E(X)]^2 = E(X^2)-[E(X)]^2
            double variance = Double.isNaN(avg) ? Double.NaN : avg2 - Math.pow(avg, 2);
            result.put(VALUE_FIELD, variance);
        }
        return result;
    }

}
