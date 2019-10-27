package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.*;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.ignite.ApacheIgniteManager;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.ConfigHolder;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.JsonTool;
import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.MD5Tool;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ModifiedExpiryPolicy;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public abstract class AbstractFunction {

    protected static String FIXED_CONTENT_BASE = "$.{fixed_content}.%s".replace("{fixed_content}",
            ConfigHolder.getString("schema.fixed_content"));
    protected static final String SPLIT_SIGN = "_";

    protected static final String ID_GENERATOR_SEQ = "ID_GENERATOR_SEQ";

    protected static final String VALUE_FIELD = "value";

    protected static final int RETRY_TIMES = 30;

    protected final String op;

    public AbstractFunction(String op) {
        this.op = op;
    }

    public String getOp() {
        return op;
    }

    protected abstract Map<String, Object> doExecute(StreamFQL fql,
                                                     JSONObject event,
                                                     Map<String, Object> helper,
                                                     String mode) throws Exception;

    protected Map<String, Object> defaultValue(StreamFQL fql,
                                               JSONObject event,
                                               Map<String, Object> helper,
                                               String mode) throws Exception {
        Map<String, Object> result = new HashMap<>();
        result.put(VALUE_FIELD, Void.create());
        return result;
    }

    public Map<String, Object> execute(StreamFQL fql,
                                       JSONObject event,
                                       Map<String, Object> helper,
                                       String mode) throws Exception {
        if (isUpdateMode(mode) && !matchEventType(fql, event)) {
            return defaultValue(fql, event, helper, mode);
        } else {
            return doExecute(fql, event, helper, mode);
        }
    }

    protected String genSpace(StreamFQL fql, JSONObject event) {
        String application = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "application"), String.class);
        JSONObject appConfig = StreamFQLConfig.getConfig().getJSONObject(application);

        String space = null;
        String spaceValue = JsonTool.getValueByPath(appConfig, "$.setting.space.value", String.class);
        if (StringUtils.isNotBlank(spaceValue)) {
            space = spaceValue;
        } else {
            String spaceField = JsonTool.getValueByPath(appConfig, "$.setting.space.field", String.class);
            if (StringUtils.isNotBlank(spaceField)) {
                space = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, spaceField), String.class);
            }
        }
        if (StringUtils.isBlank(space)) {
            space = application;
        }
        return space;
    }

    protected String genSubspace(StreamFQL fql, JSONObject event) {
        StringBuilder sb = new StringBuilder(fql.getOp());
        sb.append(SPLIT_SIGN).append(fql.getWindow())
                .append(SPLIT_SIGN).append(fql.getEvent_type());

        // target
        Field target = fql.getTarget();
        sb.append(SPLIT_SIGN)
                .append(target.getField())
                .append(SPLIT_SIGN)
                .append(target.getCondition().getMethod());

        // 根据on条件，生成group名字
        final SortedMap<String, Field> onFields = Maps.newTreeMap();
        if (fql.getOn() != null) {
            for (Field field : fql.getOn()) {
                onFields.put(field.getField(), field);
            }
        }

        for (Map.Entry<String, Field> field : onFields.entrySet()) {
            sb.append(SPLIT_SIGN)
                    .append(field.getValue().getField())
                    .append(SPLIT_SIGN)
                    .append(field.getValue().getCondition().getMethod());
        }
        return sb.toString();
    }

    protected String genCacheName(StreamFQL fql, JSONObject event) {
        String space = genSpace(fql, event);
        String subspace = MD5Tool.md5ID(genSubspace(fql, event));
        return String.format("space_%s_subspace_%s", space, subspace);
    }

    protected boolean isUpdateMode(String mode) {
        return StreamFQLMode.UPDATE.equalsIgnoreCase(mode) || StreamFQLMode.UPGET.equalsIgnoreCase(mode);
    }

    protected boolean matchEventType(StreamFQL fql, JSONObject event) {
        String eventType = JsonTool.getValueByPath(event, String.format(FIXED_CONTENT_BASE, "event_type"), String.class);
        String featureEventType = fql.getEvent_type();
        return "___ALL___".equalsIgnoreCase(featureEventType) ||
                StringUtils.equalsIgnoreCase(featureEventType, eventType);
    }

    protected boolean isGetMode(String mode) {
        return StreamFQLMode.GET.equalsIgnoreCase(mode) || StreamFQLMode.UPGET.equalsIgnoreCase(mode);
    }

    protected <K, V> IgniteCache<K, V> openIgniteCache(String cacheName,
                                                       Class<K> kClass,
                                                       Class<V> vClass,
                                                       long ttlSeconds) {
        Ignite ignite = ApacheIgniteManager.instance();
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>();
        cfg.setName(cacheName);
        cfg.setCacheMode(CacheMode.PARTITIONED);
        cfg.setIndexedTypes(kClass, vClass);
        return ignite.getOrCreateCache(cfg)
                .withExpiryPolicy(new ModifiedExpiryPolicy(
                        new Duration(TimeUnit.SECONDS, ttlSeconds)));
    }

    protected IgniteAtomicSequence openIdGenerator(String cacheName) {
        return ApacheIgniteManager.instance()
                .atomicSequence(String.format("%s_%s", cacheName, ID_GENERATOR_SEQ), 0, true);
    }

    private static final String TIME_SEG_PATTERN_STR = "^[0-9]+[mhdMUTABCDEF]$";
    private static final Pattern TIME_SEG_PATTERN = Pattern.compile(TIME_SEG_PATTERN_STR);

    private static boolean isTimeSeg(String segment) {
        return TIME_SEG_PATTERN.matcher(segment).matches();
    }

    protected long ttlSeconds(String window) {
        return windowSeconds(window) + 3;
    }

    protected long windowSeconds(String window) {
        String normWindow = window;
        if (StringUtils.isBlank(window)) {
            normWindow = "1d";
        }
        if (!isTimeSeg(normWindow)) {
            throw new IllegalArgumentException(String.format("invalid time window[%s]", window));
        }

        long time = Long.parseLong(window.substring(0, window.length() - 1));
        String timeUnit = window.substring(window.length() - 1, window.length());
        switch (timeUnit) {
            case "U":
                return TimeUnit.SECONDS.toSeconds(time);
            case "T":
                return TimeUnit.SECONDS.toSeconds(time * 2);
            case "A":
                return TimeUnit.SECONDS.toSeconds(time * 5);
            case "B":
                return TimeUnit.SECONDS.toSeconds(time * 10);
            case "C":
                return TimeUnit.SECONDS.toSeconds(time * 15);
            case "D":
                return TimeUnit.SECONDS.toSeconds(time * 20);
            case "E":
                return TimeUnit.SECONDS.toSeconds(time * 25);
            case "F":
                return TimeUnit.SECONDS.toSeconds(time * 30);
            case "m":
                return TimeUnit.MINUTES.toSeconds(time);
            case "h":
                return TimeUnit.HOURS.toSeconds(time);
            case "d":
                return TimeUnit.DAYS.toSeconds(time);
            case "M":
                return TimeUnit.DAYS.toSeconds(time * 30);
            default:
                throw new IllegalArgumentException(String.format("invalid time window[%s]", window));
        }
    }

    protected long windowMilliSeconds(String window) {
        return TimeUnit.SECONDS.toMillis(windowSeconds(window));
    }

    private long timeUnitInMS(String timeUnit) {
        switch (timeUnit) {
            case "U":
                return TimeUnit.SECONDS.toMillis(1);
            case "T":
                return TimeUnit.SECONDS.toMillis(2);
            case "A":
                return TimeUnit.SECONDS.toMillis(5);
            case "B":
                return TimeUnit.SECONDS.toMillis(10);
            case "C":
                return TimeUnit.SECONDS.toMillis(15);
            case "D":
                return TimeUnit.SECONDS.toMillis(20);
            case "E":
                return TimeUnit.SECONDS.toMillis(25);
            case "F":
                return TimeUnit.SECONDS.toMillis(30);
            case "m":
                return TimeUnit.MINUTES.toMillis(1);
            case "h":
                return TimeUnit.HOURS.toMillis(1);
            case "d":
                return TimeUnit.DAYS.toMillis(1);
            case "M":
                return TimeUnit.DAYS.toMillis(30);
            default:
                throw new IllegalArgumentException(String.format("invalid timeUnit[%s]", timeUnit));
        }
    }

    protected long timeUnit(String window) {
        String normWindow = window;
        if (StringUtils.isBlank(window)) {
            normWindow = "1d";
        }
        if (!isTimeSeg(normWindow)) {
            throw new IllegalArgumentException(String.format("invalid time window[%s]", window));
        }

        return timeUnitInMS(window.substring(window.length() - 1, window.length()));
    }

    protected long roundTimestamp(long timestamp, String window) {
        long timeUnit = timeUnit(window);
        return (timestamp / timeUnit) * timeUnit;
    }

    protected long floorTimestamp(long timestamp, String window) {
        long timeUnit = timeUnit(window);
        return (timestamp / timeUnit) * timeUnit;
    }

    protected long ceilTimestamp(long timestamp, String window) {
        long timeUnit = timeUnit(window);
        return (timestamp / timeUnit + 1) * timeUnit;
    }

    protected String getWindowSegmentId(long timestamp, String window) {
        String normWindow = window;
        if (StringUtils.isBlank(window)) {
            normWindow = "1d";
        }
        if (!isTimeSeg(normWindow)) {
            throw new IllegalArgumentException(String.format("invalid time window[%s]", window));
        }

        long timeUnitInMS = timeUnitInMS(window.substring(window.length() - 1, window.length()));

        return String.format("%d_%d", timeUnitInMS, timestamp / timeUnitInMS);
    }

    protected String getStringFromConditionOrEvent(JSONObject event, Field field) {
        return String.valueOf(getValueFromConditionOrEvent(event, field));
    }

    protected String getStringFromEvent(JSONObject event, Field field) {
        return String.valueOf(getValueFromEvent(event, field));
    }

    protected Object getValueFromConditionOrEvent(JSONObject event, Field field) {
        Object value = null;
        if (Equal.class.isInstance(field.getCondition())) {
            Equal equal = (Equal) field.getCondition();
            value = equal.getValue();
        }
        if (value == null || FieldCondition.VOID.equals(value)) {
            value = getValueFromEvent(event, field);
        }
        return value;
    }

    protected double getDoubleFromConditionOrEvent(JSONObject event, Field field) {
        String valueStr = getStringFromConditionOrEvent(event, field);
        return isValidNumber(valueStr) ? new BigDecimal(valueStr).doubleValue() : 0;
    }

    protected int getIntFromConditionOrEvent(JSONObject event, Field field) {
        String valueStr = getStringFromConditionOrEvent(event, field);
        return isValidInteger(valueStr) ? new BigDecimal(valueStr).intValue() : 0;
    }

    protected Object getValueFromEvent(JSONObject event, Field field) {
        String valuePath = String.format(FIXED_CONTENT_BASE, field.getField());
        return JsonTool.getValueByPath(event, valuePath, Object.class);
    }


    protected SortedMap<String, Field> sortedOn(StreamFQL fql) {
        final SortedMap<String, Field> onFields = Maps.newTreeMap();
        if (fql.getOn() != null) {
            fql.getOn().forEach(x -> onFields.put(x.getField(), x));
        }
        return onFields;
    }

    protected boolean isNull(Object obj) {
        return obj == null || "null".equalsIgnoreCase(String.valueOf(obj));
    }

    protected boolean isNotNull(Object obj) {
        return !isNull(obj);
    }

    protected boolean isValidNumber(Object obj) {
        return NumberUtils.isNumber(String.valueOf(obj));
    }

    protected boolean isValidInteger(Object obj) {
        return NumberUtils.isDigits(String.valueOf(obj));
    }

    protected boolean isBoolean(String value) {
        return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
    }

    protected boolean isNumber(String value) {
        return StringUtils.isNumeric(value);
    }

    protected boolean isAsc(String order) {
        return "asc".equalsIgnoreCase(order);
    }

    protected boolean isDesc(String order) {
        return "desc".equalsIgnoreCase(order);
    }

    protected boolean isOrderSign(String order) {
        return isAsc(order) || isDesc(order);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractFunction function = (AbstractFunction) o;

        return op.equals(function.op);
    }

    @Override
    public int hashCode() {
        return op.hashCode();
    }
}
