package fr.insalyon.creatis.gasw.executor.kubernetes.config;

import java.io.IOException;

import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.ConfigBuilder;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.json.properties.KConfig;
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
    private CoreV1Api				coreApi;
    private BatchV1Api				batchApi;
    private StorageV1Api			storageApi;

    // K8s configuration
    private KConfig                 config;

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
        ConfigBuilder configBuilder = new ConfigBuilder(path);
        config = configBuilder.get();

        if (config == null) {
            throw new GaswException("Client creation failed");
        }
        System.err.println("Voici quelques données " + config.getVolumes().get(0).getNfsFolder() + " " + config.getVolumes().get(0).getName());
    }
    
    private void defineApis(ApiClient client) {
        coreApi = new CoreV1Api(client);
        batchApi =  new BatchV1Api(client);
        storageApi = new StorageV1Api(client);

        log.info("Apis were defined successffully");
    }
    
    /**
     * To use when use .kube local config, useful for debug and develop
     */
    private void createLocalClient() throws GaswException {
        try {
            ApiClient client = Config.fromConfig(config.getK8sKubeConfig());
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
            .setBasePath(config.getK8sAddress())
            .setVerifyingSsl(false) // may need to change in production !
            .setAuthentication(new AccessTokenAuthentication(config.getK8sToken()))
            .setCertificateAuthority(null) // may need to change in production !
            .build();
        defineApis(client);
    }
}
