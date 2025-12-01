package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import fr.insalyon.creatis.gasw.execution.GaswStatus;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimVolumeSource;
import io.kubernetes.client.openapi.models.V1Volume;
import io.kubernetes.client.openapi.models.V1VolumeMount;

@Getter
@Setter
@NoArgsConstructor
public class KJobData {

    private String workflowName;
    private String jobID;
    private String kubernetesJobID;

    private String command;
    private List<KVolume> kVolumes;

    private V1Job job;
    private V1Container container;

    private GaswStatus status;

    public void setVolumes(final List<KVolume> volumes) {
        this.kVolumes = volumes;
        container.volumeMounts(getVolumesMounts());
    }

    public void setCommand(final String command) {
        this.command = command;
        container.command(getWrappedCommand());
    }

    public void setImage(final String image) {
        container.image(image);
    }

    public void setWorkingDir(final String workingDir) {
        container.workingDir(workingDir);
    }

    /**
     * Stdout & stderr redirectors
     * 
     * @return Initial command redirected to out & err files
     */
    private List<String> getWrappedCommand() {
        final List<String> wrappedCommand = new ArrayList<>();

        final String redirectStdout = "> " + getContainerLogPath("out") + " ";
        final String redirectStderr = "2> " + getContainerLogPath("err");
        final String redirectCmd = "exec " + redirectStdout + redirectStderr + ";";

        wrappedCommand.add(0, "bash");
        wrappedCommand.add(1, "-c");
        wrappedCommand.add(2, redirectCmd + " " + getCommand());
        return wrappedCommand;
    }

    /**
     * @param extension (out or err)
     * @return file that contain the log (inside container)
     */
    private String getContainerLogPath(final String extension) {
        return "./" + extension + "/" + getJobID() + ".sh" + "." + extension;
    }

    public List<V1Volume> getVolumes() {
        return getKVolumes().stream()
                .map(vol -> new V1Volume()
                        .name(vol.getKubernetesName())
                        .persistentVolumeClaim(new V1PersistentVolumeClaimVolumeSource()
                                .claimName(vol.getClaimName())))
                .collect(Collectors.toList());
    }

    public List<V1VolumeMount> getVolumesMounts() {
        return getKVolumes().stream()
                .map(vol -> new V1VolumeMount()
                        .name(vol.getKubernetesName())
                        .mountPath(vol.getData().getMountPathContainer()))
                .collect(Collectors.toList());
    }
}