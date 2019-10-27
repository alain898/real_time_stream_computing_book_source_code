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
import java.math.BigDecimal;
import java.util.*;

public class O_Diff extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Diff.class);

    private static final int LAST_N_ODDMENT = 3;

    public O_Diff() {
        super("DIFF");
    }

    private static class DiffTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;

        public DiffTable(String name, long timestamp, double amount) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            DiffTable diffTable = (DiffTable) o;

            if (timestamp != diffTable.timestamp) return false;
            if (Double.compare(diffTable.amount, amount) != 0) return false;
            return name != null ? name.equals(diffTable.name) : diffTable.name == null;
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
        long atTime = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "c_timestamp"), Long.class,
                System.currentTimeMillis());

        long lastN = 2;

        if (isUpdateMode(mode)) {
            String target = getStringFromEvent(event, fql.getTarget());
            if (isValidNumber(target)) {
                double amount = new BigDecimal(target).doubleValue();

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                IgniteCache<String, DiffTable> cache = openIgniteCache(cacheName, String.class, DiffTable.class,
                        ttlSeconds(fql.getWindow()));

                String nameMd5 = MD5Tool.md5ID(name);
                String indexName = String.format("%s___index", nameMd5);

                int retryTimes = RETRY_TIMES;
                DiffTable newIndexRecord = new DiffTable(indexName, atTime, 0);
                boolean succeed;
                do {
                    DiffTable oldIndexRecord = cache.get(indexName);
                    if (oldIndexRecord != null) {
                        newIndexRecord.amount = oldIndexRecord.amount + 1;
                    } else {
                        oldIndexRecord = newIndexRecord;
                        cache.putIfAbsent(indexName, oldIndexRecord);
                    }
                    succeed = cache.replace(indexName, oldIndexRecord, newIndexRecord);
                    retryTimes--;
                } while (!succeed && retryTimes > 0);
                if (!succeed) {
                    throw new IllegalStateException(String.format(
                            "DIFF failed to update index[%s], featureDSL[%s], event[%s]",
                            JSONObject.toJSONString(indexName),
                            JSONObject.toJSONString(fql),
                            JSONObject.toJSONString(event)));
                }

                String newId = String.format("%s_%s", nameMd5, String.valueOf(newIndexRecord.amount));
                DiffTable record = new DiffTable(name, atTime, amount);
                cache.put(newId, record);

                String removeId = String.format("%s_%s", nameMd5,
                        String.valueOf(newIndexRecord.amount - lastN - LAST_N_ODDMENT));
                cache.remove(removeId);
            }

            result.put(VALUE_FIELD, Void.create());
        }

        if (isGetMode(mode)) {
            List<String> nameSplits = new ArrayList<>();
            for (Map.Entry<String, Field> on : onFields.entrySet()) {
                nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
            }
            String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

            IgniteCache<String, DiffTable> cache = openIgniteCache(cacheName, String.class, DiffTable.class,
                    ttlSeconds(fql.getWindow()));
            // sum query
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT top ? amount FROM DiffTable " +
                            "WHERE name = ? ORDER BY timestamp DESC");
            List<List<?>> cursor = cache.query(sumQuery.setArgs(lastN, name)).getAll();
            List<Double> list = new LinkedList<>();
            for (List<?> row : cursor) {
                if (row.get(0) != null) {
                    list.add((Double) row.get(0));
                }
            }
            double diff;
            if (list.size() >= 2) {
                diff = list.get(0) - list.get(1);
            } else {
                diff = 0;
            }

            result.put(VALUE_FIELD, diff);
        }
        return result;
    }

}
