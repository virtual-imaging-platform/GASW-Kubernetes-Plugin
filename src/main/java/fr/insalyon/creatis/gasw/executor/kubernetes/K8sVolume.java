package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.util.Arrays;
import java.util.Map;

import io.kubernetes.client.custom.Quantity;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1NFSVolumeSource;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1PersistentVolume;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaim;
import io.kubernetes.client.openapi.models.V1PersistentVolumeClaimSpec;
import io.kubernetes.client.openapi.models.V1PersistentVolumeSpec;
import io.kubernetes.client.openapi.models.V1VolumeResourceRequirements;

/**
 * K8sVolume
 */
public class K8sVolume {
	private V1PersistentVolume 		pv;
	private V1PersistentVolumeClaim pvc;
	private String 					name;
	private final K8sConfiguration	conf;

	public K8sVolume(K8sConfiguration conf, String workflowName) {
		this.conf = conf;
		this.name = workflowName;
	}

	public String getName() { return name; }
	public String getClaimName() { return name + "-claim"; }

	// LA REMPLACER PAR LE NOM DU WORKFLOW
	public String getSubMountPath() { return conf.getNFSPath() + "test/"; } 

	public void createPV () throws Exception {
		pv = new V1PersistentVolume()
            .metadata(new V1ObjectMeta().name(name))
            .spec(new V1PersistentVolumeSpec()
                .accessModes(Arrays.asList("ReadWriteMany"))
				.capacity(Map.of("storage", new Quantity("1Gi")))
            		.nfs(new V1NFSVolumeSource()
                    .path(getSubMountPath())
                    .server(conf.getNFSAddress())
                ));
		conf.getK8sCoreApi().createPersistentVolume(pv).execute();
	}
 
	public void createPVC() throws Exception {
		pvc = new V1PersistentVolumeClaim()
			.metadata(new V1ObjectMeta().name(getClaimName()).namespace(conf.getK8sNamespace()))
			.spec(new V1PersistentVolumeClaimSpec()
				.storageClassName(K8sConstants.storageClassName)
				.resources(new V1VolumeResourceRequirements()
						.requests(Map.of("storage", new Quantity("1Gi"))))
				.addAccessModesItem("ReadWriteMany")
				.volumeName(name));

		conf.getK8sCoreApi().createNamespacedPersistentVolumeClaim(conf.getK8sNamespace(), pvc).execute();
	}

	public void deletePVC() throws Exception {
		CoreV1Api api = conf.getK8sCoreApi();

		api.deleteNamespacedPersistentVolumeClaim(getClaimName(), conf.getK8sNamespace()).execute();
	}

	public void deletePV() throws Exception {
		CoreV1Api api = conf.getK8sCoreApi();

		api.deletePersistentVolume(name).execute();
	}

	/**
	 * Check if the volume pv and pvc is bounded (the phase status)
	 * @return
	 */
	public boolean isAvailable() {
		CoreV1Api api = conf.getK8sCoreApi();
		
		try {
			V1PersistentVolume requestPv = api.readPersistentVolume(name).execute();
			V1PersistentVolumeClaim requestPvc = api.readNamespacedPersistentVolumeClaim(getClaimName(), conf.getK8sNamespace()).execute();
		
			System.err.println(requestPv.getStatus().getPhase() + " | " + requestPvc.getStatus().getPhase());
			if (requestPv.getStatus().getPhase().equals("Bound") && requestPvc.getStatus().getPhase().equals("Bound"))
				return true;
			return false;
		} catch (Exception e) {
			System.err.println("failed to check is the volume (pv and pvc) were available");
			return false;
		}
	}
}