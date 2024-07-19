package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import fr.insalyon.creatis.gasw.executor.K8sStatus;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class K8sManager {
	private String						workflowName;
	private K8sVolume 					volume;
	private volatile List<K8sExecutor> 	jobs;
	private Boolean						end;

	public K8sManager(String workflowName) {
		this.workflowName = workflowName;
		this.jobs = new ArrayList<K8sExecutor>();
	}

	public void init() throws Exception {
		K8sConfiguration conf = K8sConfiguration.getInstance();

		checkNamespace();

		volume = new K8sVolume(conf, workflowName);
		volume.createPV();
		volume.createPVC();
	}

	public void destroy() throws Exception {
		end = true;

		for (K8sExecutor job : jobs) { job.clean(); } // dev
		volume.deletePVC();
		volume.deletePV();
		volume = null;
	}

	/*
	 * Check if the k8s cluster already have the namespace
	 * if it isn't here, then it is created
	 */
	public void checkNamespace() throws Exception {
		CoreV1Api api = K8sConfiguration.getInstance().getK8sCoreApi();
		V1NamespaceList list = api.listNamespace().execute();
		String targetName = K8sConfiguration.getInstance().getK8sNamespace();
		
		for (V1Namespace ns : list.getItems()) {
			String name = ns.getMetadata().getName();
			
			if (name.equals(targetName))
				return;
		}

		V1Namespace ns = new V1Namespace()
			.metadata(new V1ObjectMeta().name(targetName));

		api.createNamespace(ns).execute();
	}
	
	public void testJob() throws Exception {
		K8sExecutor executor = new K8sExecutor("j-" + UUID.randomUUID().toString().substring(0, 8), Arrays.asList("sh", "-c", "echo migouel $RANDOM"), "busybox", volume);
		// jobs.add(executor);
		submitter(executor);
	}

	/**
	 * Create a thread instance that will launch job when ressources are available (volumes)
	 * The end variable is used to know if a thread instance has already been launched.
	 * @param exec
	 */
	private void submitter(K8sExecutor exec) {
		if (end == null) {
			end = false;
			new Thread(this.new K8sRunner()).start();
		}
		synchronized (this) {
			jobs.add(exec);
		}
	}

	class K8sRunner implements Runnable {
		private Boolean ready = false;

		@Override
		public void run() {
			try {
				loop();
			} catch (Exception e) {
				System.err.println("something bad happened during the k8sRunner " + e.getMessage()	);
			}
		}

		private void loop() throws Exception {
			while (ready == false) {
				checker();
				TimeUnit.MILLISECONDS.sleep(600);
			}
			while (end == false) {
				synchronized (this) {
					for (K8sExecutor exec : jobs) {
						if (exec.getStatus() == K8sStatus.UNSUBMITED) {
							exec.start();
						}
					}
				}
				TimeUnit.MILLISECONDS.sleep(600);
			}
		}

		private synchronized void checker() {
			if (!ready && volume.isAvailable())
				ready = true;
		}
	}
}
 