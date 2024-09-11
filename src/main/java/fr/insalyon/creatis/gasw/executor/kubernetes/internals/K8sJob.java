package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.K8sMonitor;
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
import io.kubernetes.client.openapi.models.V1SecurityContext;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

public class K8sJob {
    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private final K8sConfiguration 	conf;

    private String					workflowName;
    private String 					jobID;
    private String					kubernetesJobID;

    private String       			command;
    private List<K8sVolume>         volumes;

    private V1Job 					job;
    private V1Container             container;

    private GaswStatus              status;
    private boolean					terminated = false;


    public K8sJob(String jobID, String workflowName) {
        conf = K8sConfiguration.getInstance();
        this.jobID = jobID;
        this.workflowName = workflowName;
        this.status = GaswStatus.UNDEFINED;

        setKubernetesJobID(jobID);

        this.container = new V1Container()
            .name(kubernetesJobID)    
            .securityContext(new V1SecurityContext().privileged(true));
    }

    public String getJobID() {
        return jobID;
    }

    public void setStatus(GaswStatus status) {
        this.status = status;
    }

    public void setTerminated() {
        terminated = true;
    }

    public boolean isTerminated() {
        return terminated;
    }

    public void setVolumes(List<K8sVolume> volumes) {
        this.volumes = volumes;
        container.volumeMounts(getVolumesMounts());
    }

    public void setImage(String image) {
        container.image(image);
    }

    public void setCommand(String command) {
        this.command = command;
        container.command(getWrappedCommand());
    }

    public void setWorkingDir(String workingDir) {
        container.workingDir(workingDir);
    }

    private void setKubernetesJobID(String baseName) {
        for (int i = baseName.length() - 1; i > 0; i--) {
            if ( ! Character.isDigit(baseName.charAt(i))) {
                kubernetesJobID = baseName.substring(i + 1, baseName.length());
                break;
            }
        }
        kubernetesJobID = workflowName.toLowerCase() + "-" + kubernetesJobID;
    }

     /**
     * Stdout & stderr redirectors
     * @return Initial command redirected to out & err files
     */
    private List<String> getWrappedCommand() {
        List<String> wrappedCommand = new ArrayList<String>();

        String redirectStdout = "> " + getContainerLogPath("out") + " ";
        String redirectStderr = "2> " + getContainerLogPath("err");
        String redirectCmd = "exec " + redirectStdout + redirectStderr + ";";

        wrappedCommand.add(0, "bash");
        wrappedCommand.add(1, "-c");
        wrappedCommand.add(2, redirectCmd + " " + command);
        // System.err.println("voici la command original : " + command.toString());
        // System.err.println("voici la wrapped command : " + wrappedCommand.toString());

        // List<String> fixed = new ArrayList<String>();
        // fixed.add("/bin/sh");
        // fixed.add("-c");
        // fixed.add("sleep 50000");
        // return fixed;
        return wrappedCommand;
    }

    /**
     * @param extension (out or err)
     * @return file that contain the log (inside container)
     */
    private String getContainerLogPath(String extension) {
        return "./" + extension + "/" + jobID + ".sh" + "." + extension;
    }

    private List<V1Volume> getVolumes() {
        List<V1Volume> volumesConverted = new ArrayList<V1Volume>();

        for (K8sVolume vol : volumes) {
            V1Volume item = new V1Volume()
                .name(vol.getKubernetesName())
                .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                    .claimName(vol.getClaimName()));

            volumesConverted.add(item);
        }
        return volumesConverted;
    }

    private List<V1VolumeMount> getVolumesMounts() {
        List<V1VolumeMount> volumesMounts = new ArrayList<V1VolumeMount>();

        for (K8sVolume vol : volumes) {
            V1VolumeMount item = new V1VolumeMount()
                .name(vol.getKubernetesName())
                .mountPath(K8sConstants.mountPathContainer + vol.getName());

            volumesMounts.add(item);
        }
        return volumesMounts;
    }

    private GaswStatus getStatusRequest(BatchV1Api api) {
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
            logger.info("Impossible de récuperer l'état du job" + e.getStackTrace());
            return GaswStatus.UNDEFINED;
        }
    }

    /**
     * This create the V1Job item and configure alls specs
     * @apiNote Can be easilly upgraded to List<V1Container>
     * @param container
     */
    public void configure() {
        V1ObjectMeta meta = new V1ObjectMeta().name(kubernetesJobID).namespace(conf.getK8sNamespace());

        V1PodSpec podSpec = new V1PodSpec()
            .containers(Arrays.asList(container))
            .restartPolicy("Never")
            .volumes(getVolumes());

        V1PodTemplateSpec podspecTemplate = new V1PodTemplateSpec().spec(podSpec);

        V1JobSpec jobSpec = new V1JobSpec()
            .ttlSecondsAfterFinished(K8sConstants.ttlJob)
            .template(podspecTemplate)
            .backoffLimit(0);

        job = new V1Job()
            .spec(jobSpec)
            .metadata(meta);
        setStatus(GaswStatus.NOT_SUBMITTED);
    }

    /**
     * Send the request against the kubernetes cluster to create the job
     * @throws ApiException
     */
    public void start() throws ApiException {
        BatchV1Api api = conf.getK8sBatchApi();

        if (job == null) {
            logger.error("Impossible to start job value is null (may not be configured)");
        } else {
            api.createNamespacedJob(conf.getK8sNamespace(), job).execute();
            setStatus(GaswStatus.SUCCESSFULLY_SUBMITTED);
        }
    }

    /**
     * Kill method stop running pods and erase job for k8s api memory.
     * @throws ApiException
     */
    public void kill() throws ApiException {
        BatchV1Api api = conf.getK8sBatchApi();

        if (status == GaswStatus.NOT_SUBMITTED)
            return ;
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
        if (job != null && isTerminated())
            kill();
    }

    /**
     * @apiNote The function retry X times with a sleep of Y depending on K8sConstants.
     * @return GaswStatus.UNDEFINED means that the job weren't configured
     */
    public GaswStatus getStatus() {
        BatchV1Api api = conf.getK8sBatchApi();
        GaswStatus retrivedStatus;

        if (status == GaswStatus.STALLED)
            return status;
        if (job != null) {
            if (status == GaswStatus.NOT_SUBMITTED)
                return status;
            for (int i = 0; i < K8sConstants.statusRetry; i++) {
                retrivedStatus = getStatusRequest(api);

                if (retrivedStatus != GaswStatus.UNDEFINED)
                    return retrivedStatus;
                Utils.sleepNException(K8sConstants.statusRetryWait);
            }
            return GaswStatus.STALLED;
        }
        return GaswStatus.UNDEFINED;
    }

    /**
     * @implNote Should be adapted if multiple containers / pods per job
     */
    public Integer getExitCode() {
        CoreV1Api coreApi = conf.getK8sCoreApi();
        String jobName = job.getMetadata().getName();

        if (job == null)
            return 1;
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
}
