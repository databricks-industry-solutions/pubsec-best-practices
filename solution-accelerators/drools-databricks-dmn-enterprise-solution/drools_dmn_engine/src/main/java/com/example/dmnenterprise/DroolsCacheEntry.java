package com.example.dmnenterprise;

import org.kie.dmn.api.core.DMNModel;
import org.kie.dmn.api.core.DMNRuntime;

/**
 * Immutable holder for compiled Drools DMN runtime artifacts.
 *
 * <p>
 * This object is stored in the executor-side cache so the DMN XML does not need
 * to be parsed and compiled for every input row.</p>
 */
public class DroolsCacheEntry {

    private final DMNRuntime runtime;
    private final DMNModel model;

    /**
     * Creates a cache entry for one compiled DMN model.
     *
     * @param runtime compiled Drools DMN runtime
     * @param model compiled DMN model resolved by namespace and model name
     */
    public DroolsCacheEntry(DMNRuntime runtime, DMNModel model) {
        this.runtime = runtime;
        this.model = model;
    }

    /**
     * @return compiled DMN runtime used to evaluate rows
     */
    public DMNRuntime getRuntime() {
        return runtime;
    }

    /**
     * @return compiled DMN model used during evaluation
     */
    public DMNModel getModel() {
        return model;
    }
}
