package fr.insalyon.creatis.gasw.executor.kubernetes.config;

public class K8sConstants {

	// Plugin
	public static String storageClassName = "";
	public static String mountPathContainer = "/app/";
	public static String subLogPath = "";
	public static Integer ttlJob = 500;

	// GASW
	public static String EXECUTOR_NAME = "Kubernetes";
}
