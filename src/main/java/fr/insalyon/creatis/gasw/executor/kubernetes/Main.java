package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.UUID;

/**
 * Main
 */
public class Main {

	public static void main(String[] args) {
		K8sConfiguration.getInstance().init("config.json", "wow-workflow-item");
	}
}