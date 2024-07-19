package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.concurrent.TimeUnit;

/**
 * Main
 */
public class Main {

	public static void main(String[] args) {
		try {
			K8sConfiguration.getInstance().init("config.json");
			K8sManager manager = new K8sManager("wow-workflow-item");
			K8sManager manager2 = new K8sManager("wow-workflow-item2");

			
			manager.init();
			manager2.init();
			manager.testJob();
			manager2.testJob();
			manager2.testJob();

			TimeUnit.SECONDS.sleep(20);
			manager.destroy();
			manager2.destroy();

		} catch (Exception e) {
			System.err.println("system error : " + e.getMessage());
		}

	}
}