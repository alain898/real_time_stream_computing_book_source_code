package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Equal;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.DynamicHistogram;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.MD5Tool;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

public class O_Histogram extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Histogram.class);

    public O_Histogram() {
        super("HIST");
    }

    private static class HistogramTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private byte[] value;

        public HistogramTable(String name, long timestamp, byte[] value) {
            this.name = name;
            this.timestamp = timestamp;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            HistogramTable that = (HistogramTable) o;

            if (timestamp != that.timestamp) return false;
            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            return Arrays.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + Arrays.hashCode(value);
            return result;
        }
    }


    @Override
    public Map<String, Object> doExecute(StreamFQL fql,
                                         JSONObject event,
                                         Map<String, Object> helper,
                                         String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();

        // get lastN and on
        if (fql.getOn().size() < 1) {
            throw new IllegalArgumentException(String.format(
                    "op[%s] invalid on[%s], featureDSL[%s], event[%s]",
                    fql.getOp(),
                    JSONObject.toJSONString(fql.getOn()),
                    JSONObject.toJSONString(fql),
                    JSONObject.toJSONString(event)));
        }

        Field binsField = fql.getOn().get(0);
        if (binsField == null
                || !Equal.class.isInstance(binsField.getCondition())
                || !isNumber(((Equal) binsField.getCondition()).getValue())) {
            throw new IllegalArgumentException(String.format(
                    "op[%s] invalid on[%s], featureDSL[%s], event[%s]",
                    fql.getOp(),
                    JSONObject.toJSONString(fql.getOn()),
                    JSONObject.toJSONString(fql),
                    JSONObject.toJSONString(event)));
        }

        int bins = Integer.parseInt(((Equal) binsField.getCondition()).getValue());

        final SortedMap<String, Field> onFields = Maps.newTreeMap();
        List<Field> onList = new ArrayList<>(fql.getOn());
        onList = onList.subList(1, onList.size());
        onList.forEach(x -> onFields.put(x.getField(), x));

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
                float amount = new BigDecimal(target).floatValue();

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, HistogramTable> cache = openIgniteCache(cacheName, String.class, HistogramTable.class,
                        ttlSeconds(fql.getWindow()));

                String id = String.format("%s_%s", MD5Tool.md5ID(name), windowSegmentId);

                int retryTimes = RETRY_TIMES;
                HistogramTable newIndexRecord = new HistogramTable(name, atTime,
                        new DynamicHistogram(bins).add(new DynamicHistogram.Bin(amount, 1)).toBytes());
                boolean succeed;
                do {
                    HistogramTable oldIndexRecord = cache.get(id);
                    if (oldIndexRecord != null) {
                        newIndexRecord.value = DynamicHistogram.fromBytes(bins, oldIndexRecord.value)
                                .add(new DynamicHistogram.Bin(amount, 1)).toBytes();
                    } else {
                        oldIndexRecord = newIndexRecord;
                        cache.putIfAbsent(id, oldIndexRecord);
                    }
                    succeed = cache.replace(id, oldIndexRecord, newIndexRecord);
                    retryTimes--;
                } while (!succeed && retryTimes > 0);
                if (!succeed) {
                    throw new IllegalArgumentException(String.format(
                            "op[%s] invalid on[%s], featureDSL[%s], event[%s]",
                            fql.getOp(),
                            JSONObject.toJSONString(fql.getOn()),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }
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

            IgniteCache<String, HistogramTable> cache = openIgniteCache(cacheName, String.class, HistogramTable.class,
                    ttlSeconds(fql.getWindow()));
            // sum query
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT value FROM HistogramTable " +
                            "WHERE name = ? and timestamp > ? and timestamp <= ?");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(name, startTime, atTime)).getAll();
            DynamicHistogram dynamicHistogram = new DynamicHistogram(bins);
            for (List<?> row : cursor) {
                if (row.get(0) != null) {
                    dynamicHistogram = dynamicHistogram.merge(DynamicHistogram.fromBytes(bins, (byte[]) (row.get(0))));
                }
            }

            StreamFQLResult streamFQLResult = new StreamFQLResult();
            streamFQLResult.setFql(fql);
            streamFQLResult.setResult(dynamicHistogram.getBins());
            result.put(VALUE_FIELD, streamFQLResult);
        }

        return result;
    }
}
