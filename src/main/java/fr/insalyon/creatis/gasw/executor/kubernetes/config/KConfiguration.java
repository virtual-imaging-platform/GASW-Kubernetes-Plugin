package fr.insalyon.creatis.gasw.executor.kubernetes.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import org.json.JSONObject;

import fr.insalyon.creatis.gasw.GaswException;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.StorageV1Api;
import io.kubernetes.client.util.ClientBuilder;
import io.kubernetes.client.util.Config;
import io.kubernetes.client.util.credentials.AccessTokenAuthentication;
import lombok.Getter;
import lombok.extern.log4j.Log4j;


@Getter @Log4j
public class KConfiguration {

    private static KConfiguration instance;

    // K8s objects
    private CoreV1Api				k8sCoreApi;
    private BatchV1Api				k8sBatchApi;
    private StorageV1Api			k8sStorageApi;

    // K8s configuration
    private String					k8sAddress;
    private String					k8sToken;
    private String					k8sNamespace;

    // NFS
    private String					nfsAddress;
    private String					nfsPath;

    public static KConfiguration getInstance() {
        if (instance == null)
            instance = new KConfiguration();
        return instance;
    }

    public void init(String configurationFile) throws GaswException {
        loadConfiguration(configurationFile);
        createLocalClient();
    }

    private void loadConfiguration(String path) throws GaswException {
        try {
            String content = Files.readString(Paths.get(path));
            Map<String, Object> map = new JSONObject(content).toMap();
    
            k8sAddress = map.get("k8s_address").toString();
            k8sToken = map.get("k8s_token").toString();
            k8sNamespace = map.get("k8s_namespace").toString();
            nfsAddress = map.get("nfs_address").toString();
            nfsPath = map.get("nfs_path").toString();

            log.info("Configuration file was loaded successfully !");
        } catch (IOException e) {
            log.error(e.getStackTrace(), e);
            throw new GaswException("Client creation failed");
        }
    }
    
    private void defineApis(ApiClient client) {
        k8sCoreApi = new CoreV1Api(client);
        k8sBatchApi =  new BatchV1Api(client);
        k8sStorageApi = new StorageV1Api(client);

        log.info("Apis were defined successffully");
    }
    
    /**
     * To use when use .kube local config, useful for debug and develop
     */
    private void createLocalClient() throws GaswException {
        try {
            ApiClient client = Config.fromConfig(KConstants.kubeConfig);
            Configuration.setDefaultApiClient(client);
            defineApis(client);
        } catch (IOException e) {
            log.error(e.getStackTrace(), e);
            throw new GaswException("Client creation failed");
        }
    }

    /**
     * To use in production mode with generated K8s credentials
     * @implNote You should have an admin access to the cluster otherwise bad things could happen.
     */
    private void createRemoteClient() {
        ApiClient client = new ClientBuilder()
            .setBasePath(k8sAddress)
            .setVerifyingSsl(false) // may need to change in production !
            .setAuthentication(new AccessTokenAuthentication(k8sToken))
            .setCertificateAuthority(null) // may need to change in production !
            .build();
        defineApis(client);
    }
}
