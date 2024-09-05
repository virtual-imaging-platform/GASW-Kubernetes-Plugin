package fr.insalyon.creatis.gasw.executor.kubernetes.config;

public class K8sConstants {

    // Plugin
    public static String kubeConfig = "/var/www/cgi-bin/kubernetes/kube_config";
    public static String pluginConfig = "/var/www/cgi-bin/kubernetes/config.json"; 
    public static String storageClassName = "";
    public static String mountPathContainer = "/var/www/html/workflows/";
    public static String subLogPath = "";
    public static Integer ttlJob = 500;

    // GASW
    public static String EXECUTOR_NAME = "Kubernetes";
}
