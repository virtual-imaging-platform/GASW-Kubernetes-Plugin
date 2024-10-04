package fr.insalyon.creatis.gasw.executor.kubernetes.config;

public class KConstants {

    // Plugin
    final public static String pluginConfig = "/var/www/cgi-bin/m2Server-gasw3/conf/kubernetes_plugin.json";
    final public static String workflowsLocation = "/var/www/html/workflows/";

    // GASW
    final public static String EXECUTOR_NAME = "Kubernetes";

    // Kubernetes HTTP CODE
    final public static int NOT_FOUND_CODE = 404;
}
