package com.example.dmn;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.kie.api.io.Resource;
import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNDecisionResult;
import org.kie.dmn.api.core.DMNModel;
import org.kie.dmn.api.core.DMNResult;
import org.kie.dmn.api.core.DMNRuntime;
import org.kie.dmn.core.internal.utils.DMNRuntimeBuilder;

public class DmnMapEngine {

    private static final ThreadLocal<Map<String, DMNRuntime>> RUNTIME_CACHE
            = ThreadLocal.withInitial(HashMap::new);

    public static Map<String, String> score(
            Map<String, String> inputMap,
            String dmnXml,
            String namespace,
            String modelName,
            String decisionNamesPipe
    ) {
        DMNRuntime runtime = getRuntime(dmnXml);

        DMNModel model = runtime.getModel(namespace, modelName);

        if (model == null) {
            throw new IllegalStateException(
                    "DMN model not found. namespace=" + namespace + ", modelName=" + modelName
            );
        }

        if (model.hasErrors()) {
            throw new IllegalStateException("DMN model has errors: " + model.getMessages());
        }

        DMNContext ctx = runtime.newContext();

        if (inputMap != null) {
            for (Map.Entry<String, String> e : inputMap.entrySet()) {
                ctx.set(e.getKey(), parseValue(e.getValue()));
            }
        }

        DMNResult result = runtime.evaluateAll(model, ctx);

        if (result.hasErrors()) {
            throw new RuntimeException("DMN evaluation failed: " + result.getMessages());
        }

        Map<String, String> out = new HashMap<>();

        String[] decisionNames
                = decisionNamesPipe == null || decisionNamesPipe.isEmpty()
                ? new String[0]
                : decisionNamesPipe.split("\\|", -1);

        for (String decisionName : decisionNames) {
            DMNDecisionResult decisionResult = result.getDecisionResultByName(decisionName);
            Object value = decisionResult == null ? null : decisionResult.getResult();
            out.put(decisionName, value == null ? null : value.toString());
        }

        return out;
    }

    private static DMNRuntime getRuntime(String dmnXml) {
        String cacheKey = String.valueOf(dmnXml.hashCode());

        Map<String, DMNRuntime> cache = RUNTIME_CACHE.get();

        if (!cache.containsKey(cacheKey)) {
            Resource resource = org.kie.internal.io.ResourceFactory
                    .newByteArrayResource(dmnXml.getBytes(StandardCharsets.UTF_8))
                    .setSourcePath("dynamic-" + UUID.randomUUID() + ".dmn");

            DMNRuntime runtime = DMNRuntimeBuilder
                    .fromDefaults()
                    .buildConfiguration()
                    .fromResources(Collections.singletonList(resource))
                    .getOrElseThrow(e -> new RuntimeException("Failed to build DMN runtime from XML", e));

            cache.put(cacheKey, runtime);
        }

        return cache.get(cacheKey);
    }

    private static Object parseValue(String raw) {
        if (raw == null || raw.trim().isEmpty() || "__DMN_NULL__".equals(raw.trim())) {
            return null;
        }

        String s = raw.trim();

        if ("true".equalsIgnoreCase(s)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(s)) {
            return Boolean.FALSE;
        }

        try {
            if (!s.contains(".") && !s.contains("e") && !s.contains("E")) {
                return Integer.valueOf(s);
            }
        } catch (Exception ignored) {
        }

        try {
            return Double.valueOf(s);
        } catch (Exception ignored) {
        }

        return raw;
    }
}
