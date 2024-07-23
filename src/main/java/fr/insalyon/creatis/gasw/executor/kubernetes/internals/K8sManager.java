package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConfiguration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class K8sManager {
	private String						workflowName;
	private K8sVolume 					volume;
	private volatile ArrayList<K8sJob> 	jobs;
	private Boolean						end;

	public K8sManager(String workflowName) {
		this.workflowName = workflowName;
		this.jobs = new ArrayList<K8sJob>();
	}

	public void init() {
		K8sConfiguration conf = K8sConfiguration.getInstance();

		try {
			checkNamespace();
			
			volume = new K8sVolume(conf, workflowName);
			volume.createPV();
			volume.createPVC();
		} catch (Exception e) {
			System.err.println("Failed to init the manager ");
			e.printStackTrace();
		}

	}

	public void destroy() {
		end = true;

		try {
			volume.deletePVC();
			volume.deletePV();
			
			/* hard cleaning not prod */
			for (K8sJob job : jobs) { job.clean(); }
			volume = null;
		} catch (Exception e) {
			System.err.println("Failed to destroy the manager");
		}
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
		K8sJob executor = new K8sJob(Arrays.asList("sh", "-c", "echo migouel $RANDOM"), "busybox", volume);
		// jobs.add(executor);
		K8sJob executorbis = executor.clone();
		submitter(executor);
		submitter(executorbis);
	}

	/**
	 * Create a thread instance that will launch job when ressources are available (volumes)
	 * The end variable is used to know if a thread instance has already been launched.
	 * @param exec
	 */
	private void submitter(K8sJob exec) {
		if (end == null) {
			end = false;
			new Thread(this.new K8sRunner()).start();
		}
		synchronized (this) {
			jobs.add(exec);
		}
	}

	public void submitter(List<String> cmd, String dockerImage) {
		K8sJob exec = new K8sJob(cmd, dockerImage, volume);
		submitter(exec);
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
					for (K8sJob exec : jobs) {
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

	public ArrayList<K8sJob> getUnfinishedJobs() { 
		ArrayList<K8sJob> copy = new ArrayList<K8sJob>(jobs);

		Iterator<K8sJob> it = copy.iterator();
		while (it.hasNext()) {
   			if (it.next().isTerminated())
   				it.remove();
		}
		return copy; 
	}

	public K8sJob getJob(String jobId) {
		for (K8sJob j : jobs) {
			if (j.getJobID() == jobId)
				return j;
		}
		return null;
	} 
}
