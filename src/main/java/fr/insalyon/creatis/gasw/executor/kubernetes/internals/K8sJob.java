package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConstants;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerStateTerminated;
import io.kubernetes.client.openapi.models.V1ContainerStatus;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobSpec;
import io.kubernetes.client.openapi.models.V1JobStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1PodList;
import io.kubernetes.client.openapi.models.V1PodSpec;
import io.kubernetes.client.openapi.models.V1PodTemplateSpec;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

/**
 * K8sExecutor
 */
public class K8sJob {
    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private final K8sConfiguration 	conf;

    private String 					jobID;
	private String					lowerJobID;
    private String 					dockerImage;
    private List<String> 			command;
    private K8sVolume 				volume;
	private K8sVolume				sharedVolume;
    private V1Job 					job;
    private boolean 				submited = false;
    private boolean					terminated = false;

    public K8sJob(String jobID, List<String> command, String dockerImage, K8sVolume volume, K8sVolume sharedVolume) {
        conf = K8sConfiguration.getInstance();
        this.jobID = jobID;
		this.lowerJobID = jobID.toLowerCase();
        this.command = command;
        this.dockerImage = dockerImage;
        this.volume = volume;
		this.sharedVolume = sharedVolume;

        V1Container ctn = createContainer(this.dockerImage, this.command);
        configure(ctn);
    }

    /**
     * This create the V1Job item and configure alls specs
     * @apiNote Can be easilly upgrade to List<V1Container>
     * @param container
     */
    private void configure(V1Container container) {
        V1ObjectMeta meta = new V1ObjectMeta().name(lowerJobID).namespace(conf.getK8sNamespace());

        V1PodSpec podSpec = new V1PodSpec()
                .containers(Arrays.asList(container))
                .restartPolicy("Never")
                .volumes(Arrays.asList(new V1Volume()
                        .name(volume.getIDName())
                        .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                                .claimName(volume.getClaimName())),	
							new V1Volume()
						.name(sharedVolume.getIDName())
						.persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
								.claimName(sharedVolume.getClaimName())		
						)		
				));

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
                .name(lowerJobID)
                .image(dockerImage)
                .workingDir(K8sConstants.mountPathContainer + volume.getName()) // may be to change
                .volumeMounts(Arrays.asList(new V1VolumeMount()
                        .name(volume.getIDName())
                        .mountPath(K8sConstants.mountPathContainer + volume.getName())
						,new V1VolumeMount()
						.name(sharedVolume.getIDName())
						.mountPath(K8sConstants.mountPathContainer + sharedVolume.getName())
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

        String redirectStdout = "> " + getContainerLogPath("out") + " ";
        String redirectStderr = "2> " + getContainerLogPath("err") + " ";
        String redirectCmd = "exec " + redirectStdout + redirectStderr + ";";
        wrappedCommand.set(last, redirectCmd + " " + "sh " + wrappedCommand.get(last));
		System.err.println("voici la wrapped command : " + wrappedCommand.toString());

		// List<String> fixed = new ArrayList<String>();
		// fixed.add("/bin/sh");
		// fixed.add("-c");
		// fixed.add("sleep 50000");
		// return fixed;
        return wrappedCommand;
    }

    /**
     * @param extension (stdout or stderr)
     * @return file that contain the log (inside container)
     */
    private String getContainerLogPath(String extension) {
        return K8sConstants.mountPathContainer + volume.getName() + "/" + extension + "/" + jobID + ".sh" + "." + extension;
    }

    /**
     * @param extension (stdout or stderr)
     * @return file that contain the log (from nfs server machine)
     */
    private String getLogPath(String extension) {
        return volume.getSubMountPath() + K8sConstants.subLogPath + jobID + "." + extension;
    }

    public void start() throws ApiException {
        BatchV1Api api = conf.getK8sBatchApi();
        System.err.println("statut du volume " + volume.isAvailable());

        if (job == null || !volume.isAvailable()) {
            logger.error("Impossible to start job, isn't configured or volume not ready !");
        } else {
            api.createNamespacedJob(conf.getK8sNamespace(), job).execute();
            submited = true;
        }
    }

    /**
     * Kill method stop running pods and erase job for k8s api memory.
     * @throws ApiException
     */
    public void kill() throws ApiException {
        BatchV1Api api = conf.getK8sBatchApi();

        if (job != null) {
            api.deleteNamespacedJob(job.getMetadata().getName(), conf.getK8sNamespace())
                .propagationPolicy("Foreground").execute();
            this.job = null;
        }
    }

    /**
     * This function do the same as kill but check for the status to be terminated.
     * @throws ApiException
     */
    public void clean() throws ApiException {
        if (job != null && getStatus() == GaswStatus.COMPLETED)
            kill();
    }

    /**
     * Return a configuration copy job of the actual job (unstarted)
     */
    public K8sJob clone() {
        return new K8sJob(jobID, command, dockerImage, volume, sharedVolume);
    }

    /**
     * Develop function purpose (blocking)
     * @throws InterruptedException
     */
    public void waitForCompletion() throws InterruptedException {
        if (job != null) {
            while (getStatus() != GaswStatus.COMPLETED)
                TimeUnit.MILLISECONDS.sleep(200);
        }
    }

    public GaswStatus getStatus() {
        BatchV1Api api = conf.getK8sBatchApi();

        if (job != null) {
            if (submited == false)
                return GaswStatus.NOT_SUBMITTED;
            try {
                V1Job updatedJob = api.readNamespacedJob(job.getMetadata().getName(), conf.getK8sNamespace()).execute();
                V1JobStatus status = updatedJob.getStatus();
                if (status.getFailed() != null && status.getFailed() > 0)
                    return GaswStatus.ERROR;
                else if (status.getActive() != null && status.getActive() > 0)
                    return GaswStatus.RUNNING;
                else if (status.getSucceeded() != null && status.getSucceeded() > 0)
                    return GaswStatus.COMPLETED;
                else
                    return GaswStatus.QUEUED;
            } catch (ApiException e) {
                logger.trace("Impossible de récuperer l'état du job" + e.getStackTrace());
                return GaswStatus.UNDEFINED;
            }
        }
        return GaswStatus.UNDEFINED;
    }

    /**
     * @implNote Should be adapted if multiple containers / pods per job
     * @return
     */
    public Integer getExitCode() {
        CoreV1Api coreApi = conf.getK8sCoreApi();
        String jobName = job.getMetadata().getName();

        try {
            V1PodList podsList = coreApi.listNamespacedPod(conf.getK8sNamespace()).labelSelector("job-name=" + jobName).execute();
            V1Pod pod = podsList.getItems().get(0);
            
            for (V1ContainerStatus status : pod.getStatus().getContainerStatuses()) {
                V1ContainerStateTerminated end = status.getState().getTerminated();
                if (end != null && end.getExitCode() != 0)
                    return end.getExitCode();
            }
            return 0;
        } catch (Exception e) {
            return 1;
        }
    }

    public File getStdout() {
        return new File(getLogPath("stdout"));
    }

    public File getStderr() {
        return new File(getLogPath("stderr"));
    }

    public Map<String, String> getOutputs() {
        Map<String, String> outputs = new HashMap<String, String>();

        try {
            outputs.put("stdout", Files.readString(Paths.get(getLogPath("stdout"))));
            outputs.put("stderr", Files.readString(Paths.get(getLogPath("stderr"))));
            return (outputs);
        } catch (Exception e) {
            logger.error("Failed to read outputs files", e);
            return (outputs);
        }
    }

    public void setTerminated() { 
        terminated = true; 
    }

    public boolean isTerminated() { 
        return terminated;
    }

    public String getJobID() { 
        return jobID; 
    }
}