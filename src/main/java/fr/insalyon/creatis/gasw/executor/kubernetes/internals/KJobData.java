package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

import fr.insalyon.creatis.gasw.execution.GaswStatus;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

@Getter @Setter
public class KJobData {

    private String          workflowName;
    private String          jobID;
    private String          kubernetesJobID;
    
    private String          command;
    private List<KVolume>   kVolumes;

    private V1Job 			job;
    private V1Container     container;

    private GaswStatus      status;

    public void setVolumes(List<KVolume> volumes) {
        this.kVolumes = volumes;
        container.volumeMounts(getVolumesMounts());
    }

    public void setCommand(String command) {
        this.command = command;
        container.command(getWrappedCommand());
    }

    public void setImage(String image) {
        container.image(image);
    }

    public void setWorkingDir(String workingDir) {
        container.workingDir(workingDir);
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
        wrappedCommand.add(2, redirectCmd + " " + getCommand());
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
        return "./" + extension + "/" + getJobID()+ ".sh" + "." + extension;
    }

    public List<V1Volume> getVolumes() {
        List<V1Volume> volumesConverted = new ArrayList<V1Volume>();

        for (KVolume vol : getKVolumes()) {
            V1Volume item = new V1Volume()
                .name(vol.getKubernetesName())
                .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                    .claimName(vol.getClaimName()));

            volumesConverted.add(item);
        }
        return volumesConverted;
    }

    public List<V1VolumeMount> getVolumesMounts() {
        List<V1VolumeMount> volumesMounts = new ArrayList<V1VolumeMount>();

        for (KVolume vol : getKVolumes()) {
            V1VolumeMount item = new V1VolumeMount()
                .name(vol.getKubernetesName())
                .mountPath(vol.getData().getMountPathContainer());

            volumesMounts.add(item);
        }
        return volumesMounts;
    }
}