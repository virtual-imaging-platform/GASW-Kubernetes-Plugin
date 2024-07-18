package fr.insalyon.creatis.gasw.executor.kubernetes;

/**
 * Main
 */
public class Main {

	public static void main(String[] args) {
		try {
			K8sConfiguration.getInstance().init("config.json");
			K8sManager manager = new K8sManager("wow-workflow-item");
			manager.init();

			// manager.testJob();
			manager.destroy();

		} catch (Exception e) {
			System.err.println("system error : " + e.getMessage());
		}

	}
}