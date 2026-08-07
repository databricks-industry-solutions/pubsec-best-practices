package com.example.dmnenterprise;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.kie.api.io.Resource;
import org.kie.dmn.api.core.DMNContext;
import org.kie.dmn.api.core.DMNDecisionResult;
import org.kie.dmn.api.core.DMNModel;
import org.kie.dmn.api.core.DMNResult;
import org.kie.dmn.api.core.DMNRuntime;
import org.kie.dmn.core.internal.utils.DMNRuntimeBuilder;

/**
 * Enterprise Drools DMN execution engine for Databricks Spark workloads.
 *
 * <p>
 * This class is called from the Scala Spark UDF bridge. The Scala layer
 * passes:</p>
 *
 * <ul>
 * <li>row input values as {@code Map<String, String>}</li>
 * <li>DMN XML text</li>
 * <li>DMN namespace</li>
 * <li>DMN model name</li>
 * <li>pipe-delimited decision names to return</li>
 * </ul>
 *
 * <p>
 * The engine compiles the DMN model once per executor thread and per DMN
 * version, caches the compiled runtime/model, evaluates each row, and returns
 * decision outputs as a {@code Map<String, String>}.</p>
 */
public class DmnEnterpriseEngine {

    /**
     * Thread-local cache of compiled DMN artifacts.
     *
     * <p>
     * Why {@code ThreadLocal}:</p>
     * <ul>
     * <li>Spark executors run multiple task threads concurrently.</li>
     * <li>Drools DMN runtime objects are not shared across threads in this
     * design.</li>
     * <li>Each executor thread gets its own isolated compiled runtime/model
     * cache.</li>
     * </ul>
     *
     * <p>
     * Why {@code ConcurrentHashMap}:</p>
     * <ul>
     * <li>Each thread-local cache can store multiple DMN models/versions.</li>
     * <li>{@code computeIfAbsent} gives clean cache initialization logic.</li>
     * <li>The structure is future-safe if the caching strategy evolves.</li>
     * </ul>
     */
    private static final ThreadLocal<ConcurrentHashMap<String, DroolsCacheEntry>> THREAD_CACHE
            = ThreadLocal.withInitial(ConcurrentHashMap::new);

    /**
     * Evaluates one row against a DMN model and returns selected decision
     * outputs.
     *
     * <p>
     * This method is called once per row by the Spark UDF execution path.</p>
     *
     * @param inputMap row input map where key is DMN input name and value is
     * stringified row value
     * @param dmnXml DMN XML content
     * @param namespace DMN namespace
     * @param modelName DMN model name
     * @param decisionNamesPipe pipe-delimited decision names to extract from
     * the DMN result
     * @return map of decision name to decision result value as string
     */
    public static Map<String, String> score(
            Map<String, String> inputMap,
            String dmnXml,
            String namespace,
            String modelName,
            String decisionNamesPipe
    ) {
        DroolsCacheEntry entry = getOrCompile(dmnXml, namespace, modelName);

        DMNRuntime runtime = entry.getRuntime();
        DMNModel model = entry.getModel();

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
                = decisionNamesPipe == null || decisionNamesPipe.trim().isEmpty()
                ? new String[0]
                : decisionNamesPipe.split("\\|", -1);

        for (String decisionName : decisionNames) {
            DMNDecisionResult decisionResult = result.getDecisionResultByName(decisionName);
            Object value = decisionResult == null ? null : decisionResult.getResult();
            out.put(decisionName, value == null ? null : value.toString());
        }

        return out;
    }

    /**
     * Returns a compiled DMN cache entry or compiles and stores it if missing.
     *
     * <p>
     * Cache key is built from namespace, model name, and DMN XML hash. This
     * allows multiple DMN models and multiple versions of the same model to
     * coexist in the same executor thread cache.</p>
     *
     * @param dmnXml DMN XML content
     * @param namespace DMN namespace
     * @param modelName DMN model name
     * @return cached or newly compiled DMN artifacts
     */
    private static DroolsCacheEntry getOrCompile(
            String dmnXml,
            String namespace,
            String modelName
    ) {
        if (dmnXml == null || dmnXml.trim().isEmpty()) {
            throw new IllegalArgumentException("DMN XML is empty.");
        }

        if (namespace == null || namespace.trim().isEmpty()) {
            throw new IllegalArgumentException("DMN namespace is empty.");
        }

        if (modelName == null || modelName.trim().isEmpty()) {
            throw new IllegalArgumentException("DMN model name is empty.");
        }

        String cacheKey
                = namespace + "|" + modelName + "|" + Integer.toHexString(dmnXml.hashCode());

        ConcurrentHashMap<String, DroolsCacheEntry> cache = THREAD_CACHE.get();

        return cache.computeIfAbsent(
                cacheKey,
                key -> compileDmn(dmnXml, namespace, modelName)
        );
    }

    /**
     * Compiles the raw DMN XML into Drools runtime/model objects.
     *
     * <p>
     * This method is intentionally not called per row after the cache is warm.
     * It should run only once per executor thread per unique DMN
     * model/version.</p>
     *
     * @param dmnXml DMN XML content
     * @param namespace DMN namespace
     * @param modelName DMN model name
     * @return compiled DMN cache entry
     */
    private static DroolsCacheEntry compileDmn(
            String dmnXml,
            String namespace,
            String modelName
    ) {
        Resource resource = org.kie.internal.io.ResourceFactory
                .newByteArrayResource(dmnXml.getBytes(StandardCharsets.UTF_8))
                .setSourcePath("enterprise-dmn-" + UUID.randomUUID() + ".dmn");

        DMNRuntime runtime = DMNRuntimeBuilder
                .fromDefaults()
                .buildConfiguration()
                .fromResources(Collections.singletonList(resource))
                .getOrElseThrow(e -> new RuntimeException("Failed to compile/build DMN runtime", e));

        DMNModel model = runtime.getModel(namespace, modelName);

        if (model == null) {
            throw new RuntimeException(
                    "DMN model not found after compile. namespace="
                    + namespace + ", modelName=" + modelName
            );
        }

        if (model.hasErrors()) {
            throw new RuntimeException(
                    "Compiled DMN model has errors: " + model.getMessages()
            );
        }

        return new DroolsCacheEntry(runtime, model);
    }

    /**
     * Converts stringified Spark row values into basic Java types for FEEL
     * evaluation.
     *
     * <p>
     * The Spark notebook passes values as strings inside a map. This method
     * converts common values back to null, boolean, integer, or double before
     * setting them into the DMN context.</p>
     *
     * @param raw raw string value from Spark
     * @return converted Java value for DMN context
     */
    private static Object parseValue(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return null;
        }

        String value = raw.trim();

        if ("__DMN_NULL__".equals(value)) {
            return null;
        }

        if ("true".equalsIgnoreCase(value)) {
            return Boolean.TRUE;
        }

        if ("false".equalsIgnoreCase(value)) {
            return Boolean.FALSE;
        }

        try {
            if (!value.contains(".") && !value.contains("e") && !value.contains("E")) {
                return Integer.valueOf(value);
            }
        } catch (Exception ignored) {
        }

        try {
            return Double.valueOf(value);
        } catch (Exception ignored) {
        }

        return raw;
    }
}
