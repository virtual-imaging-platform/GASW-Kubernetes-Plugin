package fr.insalyon.creatis.gasw.executor.kubernetes;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.json.JSONObject;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.StorageV1Api;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.credentials.AccessTokenAuthentication;

/**
 * K8sConfiguration
 */
public class K8sConfiguration {

	private static K8sConfiguration instance;

	/// K8s objects
	private CoreV1Api				k8sCoreApi;
	private BatchV1Api				k8sBatchApi;
	private StorageV1Api			k8sStorageApi;

	private String					workflow;

	/// About Plugin configuration
	// K8s
	private String					k8sAddress;
	private String					k8sToken;
	private String					k8sNamespace;
	// NFS
	private String					nfsAddress;
	private String					nfsPath;

	/// Getters
	public CoreV1Api getK8sCoreApi() { return k8sCoreApi; }
	public BatchV1Api getK8sBatchApi() { return k8sBatchApi; }
	public StorageV1Api getK8sStorageApi() { return k8sStorageApi; }

	public String getK8sAddress() { return k8sAddress; }
	public String getK8sToken() { return k8sToken; }
	public String getK8sNamespace() { return k8sNamespace; }
	public String getNFSAddress() { return nfsAddress; }
	public String getNFSPath() { return nfsPath; }
	public String getWorkflow() { return workflow; }

	public static K8sConfiguration getInstance() {
		if (instance == null)
			instance = new K8sConfiguration();
		return instance;
	}

	void init(String configurationFile, String workflow) {
		this.workflow = workflow;
		loadConfiguration(configurationFile);
		createLocalClient();
	}

	void loadConfiguration(String path) {
		try {
			String content = Files.readString(Paths.get(path));
			Map<String, Object> map = new JSONObject(content).toMap();
	
			k8sAddress = map.get("k8s_address").toString();
			k8sToken = map.get("k8s_token").toString();
			k8sNamespace = map.get("k8s_namespace").toString();
			nfsAddress = map.get("nfs_address").toString();
			nfsPath = map.get("nfs_path").toString();

		} catch (Exception e){
			System.err.println("Impossible de lire le fichier de configuration ! : " + e.getMessage());
		}
	}
	
	private void defineApis(ApiClient client) {
		k8sCoreApi = new CoreV1Api(client);
		k8sBatchApi =  new BatchV1Api(client);
		k8sStorageApi = new StorageV1Api(client);
	}

	/**
	 * To use when use .kube local config, useful for debug and develop
	 */
	public void createLocalClient() {
		try {
			ApiClient client = Config.fromConfig("/home/billon/.kube/config");
			Configuration.setDefaultApiClient(client);
			defineApis(client);
		} catch (Exception e) {
			System.err.println("Something bad happened at local client creation : " + e.getMessage());
		}

	}

	/**
	 * To use in production mode with generated K8s credentials
	 * @implNote You should have an admin access to the cluster otherwise bad things could happen.
	 */
	public void createRemoteClient() {
		try {
			ApiClient client = new ClientBuilder()
				.setBasePath(k8sAddress)
				.setVerifyingSsl(false) // may need to change in production !
				.setAuthentication(new AccessTokenAuthentication(k8sToken))
				.setCertificateAuthority(null) // may need to change in production !
				.build();
			defineApis(client);
		} catch (Exception e) {
			System.err.println("Something bad happened at client creation : " + e.getMessage());
		}
	}

}