package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.Field;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.StreamFQL;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Joiner;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.*;

public class O_Sum extends AbstractFunction {
    private static final Logger logger = LoggerFactory.getLogger(O_Sum.class);

    public O_Sum() {
        super("SUM");
    }

    private static class SumTable implements Serializable {
        @QuerySqlField(index = true)
        private String name;
        @QuerySqlField(index = true)
        private long timestamp;
        @QuerySqlField
        private double amount;

        public SumTable(String name, long timestamp, double amount) {
            this.name = name;
            this.timestamp = timestamp;
            this.amount = amount;
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
            if (isValidNumber(target)) {
                double amount = new BigDecimal(target).doubleValue();

                List<String> nameSplits = new ArrayList<>();
                for (Map.Entry<String, Field> on : onFields.entrySet()) {
                    nameSplits.add(getStringFromEvent(event, on.getValue()));
                }
                String name = Joiner.on(SPLIT_SIGN).join(nameSplits);

                final IgniteAtomicSequence seq = openIdGenerator(cacheName);
                long newId = seq.incrementAndGet();
                IgniteCache<Long, SumTable> cache = openIgniteCache(cacheName, Long.class, SumTable.class,
                        ttlSeconds(fql.getWindow()));
                cache.put(newId, new SumTable(name, atTime, amount));
            }
            result.put(VALUE_FIELD, Void.create());
        }

        if (isGetMode(mode)) {
            List<String> nameSplits = new ArrayList<>();
            for (Map.Entry<String, Field> on : onFields.entrySet()) {
                nameSplits.add(getStringFromConditionOrEvent(event, on.getValue()));
            }
            String name = Joiner.on(SPLIT_SIGN).join(nameSplits);
            IgniteCache<Long, SumTable> cache = openIgniteCache(cacheName, Long.class, SumTable.class,
                    ttlSeconds(fql.getWindow()));
            SqlFieldsQuery sumQuery = new SqlFieldsQuery(
                    "SELECT sum(amount) FROM SumTable " +
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
        return result;
    }
}
