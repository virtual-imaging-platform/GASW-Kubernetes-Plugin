package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Logger;

import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1NFSVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1VolumeResourceRequirements;

public class KVolume {
    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private final KConfiguration	conf;

    private V1PersistentVolume 		pv;
    private V1PersistentVolumeClaim pvc;
    private String 					name;
    private String                  accessModes;

    /**
     * @param accessPermissions : "ReadWriteMany" or "ReadOnlyMany"
     */
    public KVolume(KConfiguration conf, String workflowName, String accessModes) {
        this.conf = conf;
        this.name = workflowName;
        this.accessModes = accessModes;
    }

    public String getName() { return name; }
    public String getClaimName() { return getKubernetesName() + "-claim"; }
    public String getSubMountPath() { return conf.getNFSPath() + getName() + "/"; } 

    /**
     * This function return in lowercase to conform to RFC 1123 (volume name)
     */
    public String getKubernetesName() {
        return name.toLowerCase();
    }
    
    public void createPV () throws ApiException {
        System.err.println("Volume creation submitted for " + getKubernetesName());
        pv = new V1PersistentVolume()
            .metadata(new V1ObjectMeta().name(getKubernetesName()))
            .spec(new V1PersistentVolumeSpec()
                .accessModes(Arrays.asList(accessModes))
                .capacity(Map.of("storage", new Quantity("1Gi")))
                    .nfs(new V1NFSVolumeSource()
                    .path(getSubMountPath())
                    .server(conf.getNFSAddress())
                ));
        conf.getK8sCoreApi().createPersistentVolume(pv).execute();
    }
 
    public void createPVC() throws ApiException {
        System.err.println("VolumeClaim creation submitted for " + getClaimName());
        pvc = new V1PersistentVolumeClaim()
            .metadata(new V1ObjectMeta().name(getClaimName()).namespace(conf.getK8sNamespace()))
            .spec(new V1PersistentVolumeClaimSpec()
                .storageClassName(KConstants.storageClassName)
                .resources(new V1VolumeResourceRequirements()
                        .requests(Map.of("storage", new Quantity("1Gi"))))
                .addAccessModesItem(accessModes)
                .volumeName(getKubernetesName()));

        conf.getK8sCoreApi().createNamespacedPersistentVolumeClaim(conf.getK8sNamespace(), pvc).execute();
    }

    public void deletePVC() throws ApiException {
        CoreV1Api api = conf.getK8sCoreApi();

        if (pvc != null)
            api.deleteNamespacedPersistentVolumeClaim(getClaimName(), conf.getK8sNamespace()).execute();
    }

    public void deletePV() throws ApiException {
        CoreV1Api api = conf.getK8sCoreApi();

        if (pv != null)
            api.deletePersistentVolume(getKubernetesName()).execute();
    }

    /**
     * Check if the volume pv and pvc is bounded (the phase status)
     * @return
     */
    public boolean isAvailable() {
        CoreV1Api api = conf.getK8sCoreApi();
        
        try {
            V1PersistentVolume requestPv = api.readPersistentVolume(getKubernetesName()).execute();
            V1PersistentVolumeClaim requestPvc = api.readNamespacedPersistentVolumeClaim(getClaimName(), conf.getK8sNamespace()).execute();
        
            System.err.println(requestPv.getStatus().getPhase() + " | " + requestPvc.getStatus().getPhase());
            if (requestPv.getStatus().getPhase().equals("Bound") && requestPvc.getStatus().getPhase().equals("Bound"))
                return true;
            return false;
        } catch (ApiException e) {
            logger.error("Failed to check if the volume PV and PVC were available !", e);
            return false;
        }
    }

    public static KVolume retrieve(String volumeName, String accessModes) {
        KConfiguration conf = KConfiguration.getInstance();
        CoreV1Api api = conf.getK8sCoreApi();

        try {
            KVolume volume = new KVolume(conf, volumeName, accessModes);
            V1PersistentVolume pv = api.readPersistentVolume(volume.getKubernetesName()).execute();
            V1PersistentVolumeClaim pvc = api.readNamespacedPersistentVolumeClaim(volume.getClaimName(), conf.getK8sNamespace()).execute();

            volume.pv = pv;
            volume.pvc = pvc;
            return volume;
        } catch (ApiException e) {
            if (e.getCode() != 404)
                logger.error("Failed to retrieve the volume " + volumeName + " exist (pv and pvc)");
            return null;
        }
    }
}