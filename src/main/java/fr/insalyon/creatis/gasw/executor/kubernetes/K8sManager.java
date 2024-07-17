package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.List;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class K8sManager {
	private K8sVolume 			volume;
	private List<K8sExecutor> 	jobs;

	public K8sManager() {}

	public void init() throws Exception {
		K8sConfiguration conf = K8sConfiguration.getInstance();

		checkNamespace();

		volume = new K8sVolume(conf);
		volume.createPV();
		volume.createPVC();
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

	// public void resubmit(K8sExecutor job)
}
 