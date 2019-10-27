package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core.functions.*;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamFQLRegistry {
    private static final Logger logger = LoggerFactory.getLogger(StreamFQLRegistry.class);

    private static final Set<AbstractFunction> FUNCTION_SET = ImmutableSet.<AbstractFunction>builder()
            .add(new O_Sum())
            .add(new O_Count())
            .add(new O_CountDistinct())
            .add(new O_Set())
            .add(new O_FlatCount())
            .add(new O_FlatSet())
            .add(new O_FlatCountDistinct())
            .add(new O_FlatSum())
            .add(new O_Max())
            .add(new O_Min())
            .add(new O_Avg())
            .add(new O_Variance())
            .add(new O_Diff())
            .add(new O_Histogram())
            .add(new F_Sum())
            .add(new F_Add())
            .add(new F_Minus())
            .add(new F_Multiply())
            .add(new F_Divide())
            .add(new F_Pow())
            .add(new F_Sqrt())
            .add(new F_Exp())
            .add(new F_Log())
            .add(new F_Abs())
            .add(new F_Quantile())
            .add(new F_Get())
            .build();

    private static final Map<String, AbstractFunction> FUNCTION_MAP = ImmutableMap.copyOf(
            FUNCTION_SET.stream().map(e -> new ImmutablePair<>(e.getOp(), e)).collect(Collectors.toList()));

    public static Map<String, AbstractFunction> showRegisteredFunctions() {
        logger.info(String.format("registered functions[%s]", JSONObject.toJSONString(FUNCTION_MAP)));
        return FUNCTION_MAP;
    }

    static {
        showRegisteredFunctions();
    }

    public static AbstractFunction getFunction(String op) {
        Preconditions.checkNotNull(op, "op is null");

        AbstractFunction function = FUNCTION_MAP.get(op);
        if (function == null) {
            showRegisteredFunctions();
            throw new UnsupportedOperationException(String.format("unsupported op[%s]", op));
        }

        return function;
    }
}
