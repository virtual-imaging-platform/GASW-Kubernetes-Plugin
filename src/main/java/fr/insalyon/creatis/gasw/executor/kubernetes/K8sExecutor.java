package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;
import io.kubernetes.client.proto.V1;

/**
 * K8sExecutor
 */
public class K8sExecutor {

	private String 					jobId;
	private List<String> 			command;
	private String 					dockerImage;
	private K8sVolume 				volume;
	private V1Job					job;
	private final K8sConfiguration 	conf;

	public K8sExecutor(String jobId, List<String> command, String dockerImage, K8sVolume volume) {
		conf = K8sConfiguration.getInstance();
		this.jobId = jobId;
		this.command = command;
		this.dockerImage = dockerImage;
		this.volume = volume;

		V1Container default = createContainer(dockerImage, command);
		configure(null);
	}

	private V1Container createContainer(String dockerImage, List<String> command) {
		V1Container ctn = new V1Container()
			.name(jobId + "-container-" + UUID.randomUUID())
			.image(dockerImage)
			.volumeMounts(Arrays.asList(new V1VolumeMount()
				.name(volume.getClaimName())
				.mountPath(K8sConstants.mountPathContainer)
				.subPath("test") // la changer par le nom du workflow (le dossier en tout cas)
			))
			.command(command);
		return ctn;
	}

	private void configure(V1Container container) {
		V1ObjectMeta meta = new V1ObjectMeta().name(jobId).namespace(conf.getK8sNamespace());

		V1PodSpec podSpec = new V1PodSpec()
			.containers(Arrays.asList(container))
			.restartPolicy("Never")
			.volumes(Arrays.asList(new V1Volume()
				.name(volume.getName())
				.persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
					.claimName(volume.getClaimName())
				)
			));

		V1PodTemplateSpec podspecTemplate = new V1PodTemplateSpec().spec(podSpec);

		V1JobSpec jobSpec = new V1JobSpec()
			.template(podspecTemplate)
			.backoffLimit(0);
		
		job = new V1Job()
			.spec(jobSpec)
			.metadata(meta);			
	}
	
	// public void start(); this has to volume.isAvailable()
	// public void kill();
	// public void wait();

	// public GaswStatus getStatus();
	// public GaswOuput getOutputs();
}