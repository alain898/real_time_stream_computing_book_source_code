package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MacroFunction {
    private final String name;
    private final List<String> args;
    private final int argsSize;
    private final List<String> replaceTokens;

    public MacroFunction(String name, List<String> args, List<String> replaceTokens) {
        this.name = name;
        this.args = args;
        this.argsSize = args.size();
        this.replaceTokens = replaceTokens;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MacroFunction macroFunction = (MacroFunction) o;

        if (argsSize != macroFunction.argsSize) return false;
        return name != null ? name.equals(macroFunction.name) : macroFunction.name == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + argsSize;
        return result;
    }

    public static MacroFunction parseMacro(String name, String value) {
        StreamFQLParser parser = new StreamFQLParser();
        Map<StreamFQL, Integer> featureDSLMap = parser.parse(name);
        if (featureDSLMap.size() != 1) {
            throw new IllegalArgumentException(String.format("invalid macro, name[%s], value[%s]", name, value));
        }
        StreamFQL nameDSL = featureDSLMap.keySet().toArray(new StreamFQL[0])[0];
        List<String> args = nameDSL.getOn().stream().map(Field::getField).collect(Collectors.toList());

        List<String> replaceTokens = parser.splitRemainDelimiter(value);

        return new MacroFunction(nameDSL.getOp(), args, replaceTokens);
    }

    public List<String> replace(List<String> value, Map<String, List<String>> functionTokenMap) {
        Preconditions.checkArgument(argsSize == value.size(), "not equal");
        Map<String, String> replaceMap = new HashMap<>();
        for (int i = 0; i < argsSize; i++) {
            replaceMap.put(args.get(i), value.get(i));
        }
        List<String> newReplaceTokens = replaceTokens.stream()
                .map(t -> replaceMap.getOrDefault(t, t))
                .collect(Collectors.toList());
        String newDsl = Joiner.on("").join(newReplaceTokens);
        StreamFQLParser parser = new StreamFQLParser();
        Map<String, List<String>> functionTokens = new HashMap<>();
        List<String> tokens = parser.parseTokens(newDsl, false);
        String topFunction = parser.parseFromTokens(null, tokens, functionTokens, null, null);
        functionTokenMap.putAll(functionTokens);
        return functionTokens.get(topFunction);
    }
}
