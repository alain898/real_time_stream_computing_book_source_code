package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alibaba.fastjson.util.FieldInfo;
import com.alibaba.fastjson.util.TypeUtils;
import com.google.common.base.Preconditions;
import com.jayway.jsonpath.JsonPath;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class JsonTool {
    public static String getValueByPath(String jsonString, String jsonPath) {
        if (jsonString == null || jsonPath == null) {
            return null;
        }

        String obj;
        try {
            obj = JsonPath.read(jsonString, jsonPath);
        } catch (Exception e) {
            return null;
        }
        return obj;
    }

    public static <T> T getValueByPath(JSONObject jsonObject, String jsonPath, Class<T> tClass, T defValue) {
        T res = getValueByPath(jsonObject, jsonPath, tClass);
        if (res != null) {
            return res;
        } else {
            return defValue;
        }
    }


    @SuppressWarnings("unchecked")
    private static Object replaceKey(Object javaObject, ParserConfig mapping,
                                     String oldPattern, String newPattern) {

        if (javaObject == null) {
            return null;
        }

        if (javaObject instanceof Map) {
            Map<Object, Object> map = (Map<Object, Object>) javaObject;

            JSONObject json = new JSONObject(map.size());

            for (Map.Entry<Object, Object> entry : map.entrySet()) {
                Object key = entry.getKey();
                String jsonKey = TypeUtils.castToString(key).replace(oldPattern, newPattern);
                Object jsonValue = replaceKey(entry.getValue(), oldPattern, newPattern);
                json.put(jsonKey, jsonValue);
            }

            return json;
        }

        if (javaObject instanceof Collection) {
            Collection<Object> collection = (Collection<Object>) javaObject;

            JSONArray array = new JSONArray(collection.size());

            for (Object item : collection) {
                Object jsonValue = replaceKey(item, oldPattern, newPattern);
                array.add(jsonValue);
            }

            return array;
        }

        Class<?> clazz = javaObject.getClass();

        if (clazz.isEnum()) {
            return ((Enum<?>) javaObject).name();
        }

        if (clazz.isArray()) {
            int len = Array.getLength(javaObject);

            JSONArray array = new JSONArray(len);

            for (int i = 0; i < len; ++i) {
                Object item = Array.get(javaObject, i);
                Object jsonValue = replaceKey(item, oldPattern, newPattern);
                array.add(jsonValue);
            }

            return array;
        }

        if (mapping.isPrimitive(clazz)) {
            return javaObject;
        }

        try {
            List<FieldInfo> getters = TypeUtils.computeGetters(clazz, null);

            JSONObject json = new JSONObject(getters.size());

            for (FieldInfo field : getters) {
                Object value = field.get(javaObject);
                Object jsonValue = replaceKey(value, oldPattern, newPattern);

                json.put(field.getName().replace(oldPattern, newPattern), jsonValue);
            }

            return json;
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new JSONException("toJSON error", e);
        }
    }

    public static <T> T getValueByPath(JSONObject jsonObject, String jsonPath, Class<T> tClass) {
        Preconditions.checkArgument(StringUtils.isNotBlank(jsonPath), "jsonPath is blank");
        Preconditions.checkNotNull(tClass, "tClass is blank");

        if (jsonObject == null) {
            return null;
        }

        Object obj;
        try {
            obj = JSONPath.eval(jsonObject, jsonPath);
        } catch (Exception e) {
            throw new RuntimeException(String.format(
                    "failed to eval jsonObject[%s], jsonPath[%s], tClass[%s]", jsonObject, jsonPath, tClass));
        }
        if (obj == null) {
            return null;
        }
        return tClass.cast(obj);
    }

    public static Object replaceKey(Object javaObject,
                                    String oldPattern, String newPattern) {
        return replaceKey(javaObject, ParserConfig.getGlobalInstance(),
                oldPattern, newPattern);
    }
}
