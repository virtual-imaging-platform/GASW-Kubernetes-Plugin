package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import fr.insalyon.creatis.gasw.executor.K8sStatus;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

/**
 * K8sExecutor
 */
public class K8sExecutor {

	private final K8sConfiguration 	conf;

	private String 					jobId;
	private String 					dockerImage;
	private List<String> 			command;
	private K8sVolume 				volume;
	private V1Job 					job;
	private boolean 				submited = false;

	public K8sExecutor(List<String> command, String dockerImage, K8sVolume volume) {
		conf = K8sConfiguration.getInstance();
		this.jobId = UUID.randomUUID().toString();
		this.command = command;
		this.dockerImage = dockerImage;
		this.volume = volume;

		V1Container ctn = createContainer(this.dockerImage, this.command);
		configure(ctn);
	}

	/**
	 * This create the V1Job item and configure alls specs
	 * @apiNote Can be easilly upgrade to List<V1Container>
	 * @param container
	 */
	private void configure(V1Container container) {
		V1ObjectMeta meta = new V1ObjectMeta().name(jobId).namespace(conf.getK8sNamespace());

		V1PodSpec podSpec = new V1PodSpec()
				.containers(Arrays.asList(container))
				.restartPolicy("Never")
				.volumes(Arrays.asList(new V1Volume()
						.name(volume.getName())
						.persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
								.claimName(volume.getClaimName()))));

		V1PodTemplateSpec podspecTemplate = new V1PodTemplateSpec().spec(podSpec);

		V1JobSpec jobSpec = new V1JobSpec()
				.ttlSecondsAfterFinished(K8sConstants.ttlJob)
				.template(podspecTemplate)
				.backoffLimit(0);

		job = new V1Job()
				.spec(jobSpec)
				.metadata(meta);
	}

	private V1Container createContainer(String dockerImage, List<String> command) {
		V1Container ctn = new V1Container()
				.name(jobId)
				.image(dockerImage)
				.workingDir(K8sConstants.mountPathContainer) // may be to change
				.volumeMounts(Arrays.asList(new V1VolumeMount()
						.name(volume.getName())
						.mountPath(K8sConstants.mountPathContainer)
				))
				.command(getWrappedCommand());
		return ctn;
	}

	/**
	 * Stdout & stderr redirectors
	 * @return Initial command redirected to out & err files
	 */
	private List<String> getWrappedCommand() {
		List<String> wrappedCommand = new ArrayList<String>(command);
		Integer last = wrappedCommand.size() - 1;

		String redirectStdout = "> " + getContainerLogPath("stdout") + " ";
		String redirectStderr = "2> " + getContainerLogPath("stderr") + " ";
		String redirectCmd = "exec " + redirectStdout + redirectStderr + ";";
		wrappedCommand.set(last, redirectCmd + " " + wrappedCommand.get(last));
		return wrappedCommand;
	}

	/**
	 * @param extension (stdout or stderr)
	 * @return file that contain the log (inside container)
	 */
	private String getContainerLogPath(String extension) {
		return K8sConstants.mountPathContainer + K8sConstants.subLogPath + jobId + "." + extension;
	}

	/**
	 * @param extension (stdout or stderr)
	 * @return file that contain the log (from nfs server machine)
	 */
	private String getLogPath(String extension) {
		return volume.getSubMountPath() + K8sConstants.subLogPath + jobId + "." + extension;
	}

	public void start() throws Exception {
		BatchV1Api api = conf.getK8sBatchApi();
		System.err.println("statut du volume " + volume.isAvailable());

		if (job == null || !volume.isAvailable()) {
			System.out.println("Impossible to start job, isn't configure or volume not ready !");
		} else {
			api.createNamespacedJob(conf.getK8sNamespace(), job).execute();
			submited = true;
		}
	}

	/**
	 * Kill method stop running pods and erase job for k8s api memory.
	 * @throws Exception
	 */
	public void kill() throws Exception {
		BatchV1Api api = conf.getK8sBatchApi();

		if (job != null) {
			System.err.println("je delete le job");
			api.deleteNamespacedJob(job.getMetadata().getName(), conf.getK8sNamespace())
				.propagationPolicy("Foreground").execute();
			this.job = null;
		}
	}

	/**
	 * This function do the same as kill but check for the status to be terminated.
	 * @throws Exception
	 */
	public void clean() throws Exception {
		if (job != null && getStatus() == K8sStatus.FINISHED)
			kill();
	}


	/**
	 * Return a configuration copy job of the actual job (unstarted)
	 */
	public K8sExecutor clone() {
		return new K8sExecutor(command, dockerImage, volume);
	}

	/**
	 * Develop function purpose (blocking)
	 * @throws Exception
	 */
	public void waitForCompletion() throws Exception {
		if (job != null) {
			while (getStatus() != K8sStatus.FINISHED)
				TimeUnit.MILLISECONDS.sleep(200);
		}
	}

	public K8sStatus getStatus() {
		BatchV1Api api = conf.getK8sBatchApi();

		if (job != null) {
			if (submited == false)
				return K8sStatus.UNSUBMITED;
			try {
				V1Job updatedJob = api.readNamespacedJob(job.getMetadata().getName(), conf.getK8sNamespace()).execute();
				V1JobStatus status = updatedJob.getStatus();
				if (status.getFailed() != null && status.getFailed() > 0)
					return K8sStatus.FAILED;
				else if (status.getActive() != null && status.getActive() > 0)
					return K8sStatus.RUNNING;
				else if (status.getSucceeded() != null && status.getSucceeded() > 0)
					return K8sStatus.FINISHED;
				else
					return K8sStatus.PENDING;
			} catch (Exception e) {
				System.err.println("impossible de récuperer l'état du job : " + e.getMessage());
				return K8sStatus.PENDING;
			}
		}
		return K8sStatus.PENDING;
	}

	public Map<String, String> getOutput() {
		Map<String, String> outputs = new HashMap<String, String>();

		try {
			outputs.put("stdout", Files.readString(Paths.get(getLogPath("stdout"))));
			outputs.put("stderr", Files.readString(Paths.get(getLogPath("stderr"))));
			return (outputs);
		} catch (Exception e) {
			System.err.println("failed to read outputs files");
			e.printStackTrace();
			return (outputs);

		}
	}
}