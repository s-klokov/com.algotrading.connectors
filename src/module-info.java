module com.algotrading.connectors {
    requires com.algotrading.base;
    requires json.simple;
    exports com.algotrading.connectors.common;
    exports com.algotrading.connectors.quik;
    exports com.algotrading.connectors.quik.execution;
}