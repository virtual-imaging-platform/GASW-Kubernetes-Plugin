package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.concurrent.TimeUnit;

import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.K8sManager;

/**
 * Main
 */
public class Main {

	public static void main(String[] args) {
		try {
			K8sConfiguration.getInstance().init("config.json");
			K8sManager manager = new K8sManager("wow-workflow-item");

			manager.init();
			manager.testJob();

			TimeUnit.SECONDS.sleep(20);
			manager.destroy();

		} catch (Exception e) {
			System.err.println("system error : " + e.getStackTrace());
		}

	}
}