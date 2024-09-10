package fr.insalyon.creatis.gasw.executor.kubernetes.config;

public class K8sConstants {

    // Plugin
    public static String kubeConfig = "/var/www/cgi-bin/kubernetes/kube_config";
    public static String pluginConfig = "/var/www/cgi-bin/kubernetes/config.json"; 
    public static String storageClassName = "";
    public static String mountPathContainer = "/var/www/html/workflows/";
    public static int ttlJob = 500;
    public static int maxRetryToPush = 5; /* 10 seconds between each try */

    // GASW
    public static String EXECUTOR_NAME = "Kubernetes";
}
