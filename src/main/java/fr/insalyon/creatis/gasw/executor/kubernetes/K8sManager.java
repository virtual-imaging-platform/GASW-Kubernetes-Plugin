package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import fr.insalyon.creatis.gasw.executor.K8sStatus;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class K8sManager {
	private K8sVolume 			volume;
	private String				workflowName;
	private List<K8sExecutor> 	jobs;

	public K8sManager(String workflowName) {
		this.workflowName = workflowName;
	}

	public void init() throws Exception {
		K8sConfiguration conf = K8sConfiguration.getInstance();

		checkNamespace();

		volume = new K8sVolume(conf, workflowName);
		volume.createPV();
		volume.createPVC();

		while (!volume.isAvailable())
			TimeUnit.MILLISECONDS.sleep(100); // TO DELETE (IT'S FOR LONG VOLUME CREATION)
	}

	public void destroy() throws Exception {
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
		K8sExecutor executor = new K8sExecutor("ultra-pro", Arrays.asList("sh", "-c", "echo migouel $RANDOM"), "busybox", volume);
		
		executor.start();
		
		K8sStatus status = K8sStatus.PENDING;
		while (status != K8sStatus.FINISHED) {
			status = executor.getStatus();
			System.out.println(executor.getStatus().toString());
			TimeUnit.MILLISECONDS.sleep(500);
		}
		executor.getOutput();
		executor.clean();

	}
	// public void resubmit(K8sExecutor job)
}
 