package fr.insalyon.creatis.gasw.executor.kubernetes.config;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;

public class KConstants {

    // Plugin
    public static String getPluginConfig() throws GaswException {
        return GaswConfiguration.getInstance().getConfigDir() + "/executor.json";
    }

    // GASW
    final public static String EXECUTOR_NAME = "Kubernetes";

    // Kubernetes HTTP CODE
    final public static int NOT_FOUND_CODE = 404;
}
