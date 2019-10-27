package com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.core;

import com.alain898.book.realtimestreaming.chapter11.streamfeatureextractor.tools.Base64Utils;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

public class StreamFQLParser {
    private static final Logger logger = LoggerFactory.getLogger(StreamFQLParser.class);

    private static final String TOKEN_SPLIT_PATTERN = "(\\s+)|(\\s*,\\s*)";

    private static final String TOKEN_SPLIT_KEEP_PATTERN =
            "((?<=(_TOKEN_SPLIT_PATTERN_))|(?=(_TOKEN_SPLIT_PATTERN_)))"
                    .replace("_TOKEN_SPLIT_PATTERN_", TOKEN_SPLIT_PATTERN);

    public String normalizeDSL(String dsl) {
        return dsl
                .replaceAll("\\s*,\\s*", ",")
                .replaceAll("\\s*\\(\\s*", "(")
                .replaceAll("\\s*\\)\\s*", ")")
                .replaceAll("\\s*=\\s*", "=")
                .replaceAll("\\s*\\?\\s*", "?")
                .replaceAll("\\s+", "")
                .trim();
    }

    private String genDSL4Parser(String dsl) {
        return dsl.replace("(", " ( ")
                .replace(")", " ) ")
                .trim();
    }

    public List<String> normalizeDSL(List<String> dsl) {
        if (dsl == null) {
            return null;
        }

        return dsl.stream().map(this::normalizeDSL).collect(Collectors.toList());
    }


    public Map<String, Set<StreamFQL>> parseExecutionTree(List<String> dsls) {
        return parseExecutionTree(null, dsls, false);
    }

    public List<String> splitRemainDelimiter(String dsl) {
        return Arrays.stream(genDSL4Parser(normalizeDSL(dsl)).split(TOKEN_SPLIT_KEEP_PATTERN))
                .filter(StringUtils::isNotBlank).collect(Collectors.toList());
    }

    private List<String> expandFunctionToken(Map<MacroFunction, MacroFunction> macros,
                                             Map<String, List<String>> functionTokensMap,
                                             List<String> functionTokens) {
        List<String> newFunctionTokens = new LinkedList<>(functionTokens);
        int tokenNumber = newFunctionTokens.size();
        int index = 0;
        while (index < tokenNumber) {
            String currentToken = newFunctionTokens.get(index);
            if (currentToken.startsWith("___B___") && currentToken.endsWith("___F___")) {
                List<String> newTokens = functionTokensMap.get(currentToken);
                newFunctionTokens.remove(index);
                while (newTokens.get(0).startsWith("M_")) {
                    List<String> args = newTokens.size() <= 3 ?
                            new ArrayList<>() : newTokens.subList(2, newTokens.size() - 1);
                    MacroFunction key = new MacroFunction(newTokens.get(0), args, null);
                    MacroFunction macroFunction = macros.get(key);
                    newTokens = macroFunction.replace(args, functionTokensMap);
                }
                newFunctionTokens.addAll(index, newTokens);
            }
            index++;
            tokenNumber = newFunctionTokens.size();
        }
        return newFunctionTokens;
    }

    public String parseFromTokens(
            JSONObject applicationSetting,
            List<String> tokens,
            Map<String, List<String>> functionTokens,
            Map<String, StreamFQL> featureDSLMap,
            Map<StreamFQL, Integer> functions) {
        if (functionTokens == null) {
            functionTokens = new HashMap<>();
        }
        if (featureDSLMap == null) {
            featureDSLMap = new HashMap<>();
        }
        if (functions == null) {
            functions = new HashMap<>();
        }

        List<Tuple2<StreamFQL, Integer>> functionList = new LinkedList<>();
        Stack<String> stack = new Stack<>();
        int depth = 0;
        for (String token : tokens) {
            if (")".equals(token)) {
                List<String> newFunctionTokens = new LinkedList<>();
                while (true) {
                    String lastToken = stack.pop();
                    if ("(".equals(lastToken)) {
                        break;
                    }
                    newFunctionTokens.add(lastToken);
                }
                newFunctionTokens.add(stack.pop());
                newFunctionTokens = Lists.reverse(newFunctionTokens);
                StreamFQL newFunction = genFunctionDSL(applicationSetting, newFunctionTokens);
                functionList.add(new Tuple2<>(newFunction, depth));

                newFunctionTokens.add(1, "(");
                newFunctionTokens.add(")");
                functionTokens.put(newFunction.getName(), newFunctionTokens);
                featureDSLMap.put(newFunction.getName(), newFunction);

                stack.push(newFunction.getName());
                depth -= 1;
            } else {
                stack.push(token);
                if ("(".equals(token)) {
                    depth += 1;
                }
            }
        }
        String topFunctionName = stack.pop();
        if (!stack.isEmpty()) {
            throw new IllegalStateException(String.format("invalid tokens[%s]", String.valueOf(tokens)));
        }

        for (Tuple2<StreamFQL, Integer> tuple : functionList) {
            Integer fDepth = functions.get(tuple._1());
            if (fDepth == null || tuple._2() > fDepth) {
                functions.put(tuple._1(), tuple._2());
            }
        }
        return topFunctionName;
    }

    private static class PreParseInfo {
        private String name;
        private String text_name;
        private List<String> tokens;

        public PreParseInfo(String name, String text_name, List<String> tokens) {
            this.name = name;
            this.text_name = text_name;
            this.tokens = tokens;
        }
    }

    public PreParseInfo preParse(Map<MacroFunction, MacroFunction> macros, String dsl, boolean dslNormalized) {
        Map<String, List<String>> functionTokens = new HashMap<>();
        Map<String, StreamFQL> featureDSLMap = new HashMap<>();
        List<String> tokens = parseTokens(dsl, dslNormalized);
        String topFunction = parseFromTokens(null, tokens, functionTokens, featureDSLMap, null);
        List<String> newTokens = new LinkedList<>();
        newTokens.add(topFunction);
        newTokens = expandFunctionToken(macros, functionTokens, newTokens);
        return new PreParseInfo(topFunction, featureDSLMap.get(topFunction).getText_name(), newTokens);
    }

    public Map<String, Set<StreamFQL>> parseExecutionTree(JSONObject applicationSetting,
                                                          List<String> dsls,
                                                          boolean dslNormalized) {
        if (applicationSetting == null) {
            applicationSetting = new JSONObject();
        }

        // parse macros
        Map<MacroFunction, MacroFunction> macros = new HashMap<>();
        JSONObject macrosMap = applicationSetting.getJSONObject("macros");
        if (MapUtils.isNotEmpty(macrosMap)) {
            for (Map.Entry<String, Object> entry : macrosMap.entrySet()) {
                MacroFunction macroFunction = MacroFunction.parseMacro(entry.getKey(), String.valueOf(entry.getValue()));
                macros.put(macroFunction, macroFunction);
            }
        }

        // parse dsl
        Map<String, Set<StreamFQL>> result = new HashMap<>();
        Map<StreamFQL, Tuple2<StreamFQL, Integer>> allFunctions = new HashMap<>();
        for (String elem : dsls) {
            PreParseInfo preParseInfo = preParse(macros, elem, dslNormalized);
            Map<StreamFQL, Integer> functions = new HashMap<>();
            Map<String, StreamFQL> featureDSLMap = new HashMap<>();
            String topFunction = parseFromTokens(
                    applicationSetting, preParseInfo.tokens, null, featureDSLMap, functions);
            if (MapUtils.isEmpty(functions)) {
                continue;
            }
            StreamFQL topFQL = featureDSLMap.get(topFunction);
            if (!topFQL.getName().equals(preParseInfo.name)) {
                topFQL.getAlias().add(preParseInfo.text_name);
            }

            for (Map.Entry<StreamFQL, Integer> entry : functions.entrySet()) {
                Tuple2<StreamFQL, Integer> oldFeatureDSL = allFunctions.get(entry.getKey());
                if (oldFeatureDSL == null) {
                    allFunctions.put(entry.getKey(), new Tuple2<>(entry.getKey(), entry.getValue()));
                } else {
                    if (entry.getValue() > oldFeatureDSL._2()) {
                        entry.getKey().getAlias().addAll(oldFeatureDSL._1().getAlias());
                        allFunctions.put(entry.getKey(), new Tuple2<>(entry.getKey(), entry.getValue()));
                    } else {
                        oldFeatureDSL._1().getAlias().addAll(entry.getKey().getAlias());
                    }
                }
            }
        }

        for (Tuple2<StreamFQL, Integer> entry : allFunctions.values()) {
            result.putIfAbsent(String.valueOf(entry._2()), new HashSet<>());
            result.get(String.valueOf(entry._2())).add(entry._1());
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("parse dsl[%s], result[%s]",
                    JSONObject.toJSONString(result), JSONObject.toJSONString(result)));
        }
        return result;
    }

    public Map<StreamFQL, Integer> parse(String dsl) {
        return parse(null, dsl, false);
    }

    public List<String> parseTokens(String dsl, boolean dslNormalized) {
        String normDSL = dsl;
        if (!dslNormalized) {
            normDSL = normalizeDSL(dsl);
        }

        String parserDSL = genDSL4Parser(normDSL);
        Scanner s = new Scanner(parserDSL).useDelimiter(TOKEN_SPLIT_PATTERN);
        List<String> tokens = new LinkedList<>();
        while (s.hasNext()) {
            tokens.add(s.next());
        }
        tokens = tokens.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("dsl[%s], tokens[%s]",
                    parserDSL, JSONObject.toJSONString(tokens)));
        }

        return tokens;
    }

    public Map<StreamFQL, Integer> parse(JSONObject applicationSetting, String dsl, boolean dslNormalized) {
        Map<StreamFQL, Integer> functions = new HashMap<>();
        List<String> tokens = parseTokens(dsl, dslNormalized);
        parseFromTokens(applicationSetting, tokens, null, null, functions);
        if (logger.isDebugEnabled()) {
            logger.debug(String.format("parse dsl[%s], functions[%s]", dsl, JSONObject.toJSONString(functions)));
        }
        return functions;
    }

    private static final String CONDITION_SPLIT_PATTERN = "=|\\?";

    private Field parseField(String field) {
        Scanner s = new Scanner(normalizeDSL(field)).useDelimiter(CONDITION_SPLIT_PATTERN);
        List<String> tokens = new LinkedList<>();
        while (s.hasNext()) {
            tokens.add(s.next());
        }
        tokens = tokens.stream().filter(StringUtils::isNotBlank).collect(Collectors.toList());
        Field newField = new Field();
        if (tokens.size() == 1) {
            newField.setField(tokens.get(0));
            newField.setCondition(new Equal(FieldCondition.VOID));
        } else if (tokens.size() == 2) {
            // TODO: only EqualCondition supported now
            newField.setField(tokens.get(0));
            newField.setCondition(new Equal(tokens.get(1)));
        } else {
            throw new IllegalArgumentException(String.format("invalid filed[%s]", field));
        }
        return newField;
    }

    private List<Field> getOnDefault(JSONObject applicationSetting) {
        List<Field> onDefault = new ArrayList<>();
        if (applicationSetting == null) {
            return onDefault;
        }
        JSONArray onDefaultArray = applicationSetting.getJSONArray("on_default");
        if (onDefaultArray != null) {
            for (Object o : onDefaultArray) {
                JSONObject field = (JSONObject) o;
                Field newField = new Field();
                newField.setField(field.getString("field"));
                newField.setCondition(new Equal(FieldCondition.VOID));
                onDefault.add(newField);
            }
        }
        return onDefault;
    }

    private String decode(String encodeStr) {
        if (encodeStr.startsWith("___B___") && encodeStr.endsWith("___F___")) {
            encodeStr = encodeStr.replace("___B___", "").replace("___F___", "");
            String decoded = Base64Utils.decodeBase64(encodeStr);
            String[] splits = decoded.split("&");
            List<String> strings = Arrays.stream(splits).map(this::decode).collect(Collectors.toList());
            return String.format("%s(%s)", strings.get(0),
                    Joiner.on(",").join(strings.subList(1, strings.size())));
        } else {
            return encodeStr;
        }
    }

    private boolean checkFieldExists(List<Field> onList, Field on) {
        if (CollectionUtils.isEmpty(onList)) {
            return false;
        }
        for (Field e : onList) {
            if (StringUtils.equals(e.getField(), on.getField())) {
                return true;
            }
        }
        return false;
    }

    private boolean isValidOpChar(String op) {
        return CharMatcher.javaUpperCase()
                .or(CharMatcher.DIGIT)
                .or(CharMatcher.anyOf("_"))
                .matchesAllOf(op);
    }

    private StreamFQL genFunctionDSL(JSONObject applicationSetting, List<String> newFunctionTokens) {
        Preconditions.checkNotNull(newFunctionTokens, "newFunctionTokens is null");
        if (newFunctionTokens.size() < 1) {
            throw new IllegalArgumentException(String.format("invalid newFunctionTokens[%s]",
                    JSONObject.toJSONString(newFunctionTokens)));
        }

        String op = newFunctionTokens.get(0);
        if (!isValidOpChar(op)) {
            throw new IllegalArgumentException(String.format("invalid op[%s] in newFunctionTokens[%s]",
                    op, JSONObject.toJSONString(newFunctionTokens)));
        }

        StreamFQL fql = new StreamFQL();
        fql.setOp(op);
        if (op.startsWith("M_")) {
            fql.setWindow(null);
            fql.setEvent_type(null);
            fql.setTarget(null);
            List<Field> onList = new ArrayList<>();
            for (int i = 1; i < newFunctionTokens.size(); i++) {
                Field newField = parseField(newFunctionTokens.get(i));
                onList.add(newField);
            }
            fql.setOn(onList);
        } else if (op.startsWith("F_")) {
            fql.setWindow(null);
            fql.setEvent_type("___ALL___");
            fql.setTarget(null);
            List<Field> onList = new ArrayList<>();
            for (int i = 1; i < newFunctionTokens.size(); i++) {
                Field newField = parseField(newFunctionTokens.get(i));
                onList.add(newField);
            }
            fql.setOn(onList);
        } else if (newFunctionTokens.size() >= 4) {
            fql.setWindow(newFunctionTokens.get(1));
            fql.setEvent_type(newFunctionTokens.get(2));
            fql.setTarget(parseField(newFunctionTokens.get(3)));
            List<Field> onList = new ArrayList<>();
            for (int i = 4; i < newFunctionTokens.size(); i++) {
                Field newField = parseField(newFunctionTokens.get(i));
                if (checkFieldExists(onList, newField)) {
                    throw new IllegalArgumentException(String.format(
                            "field[%s] duplicated, applicationSetting[%s], newFunctionTokens[%s]",
                            JSONObject.toJSONString(newField),
                            JSONObject.toJSONString(applicationSetting),
                            JSONObject.toJSONString(newFunctionTokens)));
                }
                onList.add(newField);
            }
            List<Field> onDefaults = getOnDefault(applicationSetting);
            for (Field onDefault : onDefaults) {
                if (checkFieldExists(onList, onDefault)) {
                    throw new IllegalArgumentException(String.format(
                            "onDefault[%s] duplicated, applicationSetting[%s], newFunctionTokens[%s]",
                            JSONObject.toJSONString(onDefault),
                            JSONObject.toJSONString(applicationSetting),
                            JSONObject.toJSONString(newFunctionTokens)));
                }
                onList.add(onDefault);
            }
            fql.setOn(onList);
        } else {
            throw new IllegalArgumentException(String.format("invalid newFunctionTokens[%s]",
                    JSONObject.toJSONString(newFunctionTokens)));
        }

        String name = String.format("___B___%s___F___",
                Base64Utils.encodeBase64(Joiner.on("&").join(newFunctionTokens)));
        fql.setName(name);
        List<String> tokens = newFunctionTokens.stream().map(this::decode).collect(Collectors.toList());
        String textName = String.format("%s(%s)", tokens.get(0),
                Joiner.on(",").join(tokens.subList(1, tokens.size())));
        fql.setText_name(textName);
        return fql;
    }

}
