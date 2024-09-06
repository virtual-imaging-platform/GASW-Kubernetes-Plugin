package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.bouncycastle.jcajce.provider.symmetric.AES.KeyGen192;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.google.protobuf.Api;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswConstants;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.execution.GaswStatus;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConfiguration;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Namespace;
import io.kubernetes.client.openapi.models.V1NamespaceList;
import io.kubernetes.client.openapi.models.V1ObjectMeta;

public class K8sManager {
    private static final Logger logger = Logger.getLogger("fr.insalyon.creatis.gasw");
    private String						workflowName;
    private K8sVolume 					volume;
	private K8sVolume					sharedVolume;
    private volatile ArrayList<K8sJob> 	jobs;
    private Boolean						end;

    public K8sManager(String workflowName) {
        this.workflowName = workflowName;
        this.jobs = new ArrayList<K8sJob>();
    }

    public void init() {
        K8sConfiguration conf = K8sConfiguration.getInstance();
        System.err.println("K8s Manager init with " + workflowName);
        try {
            checkNamespace();
			checkSharedVolume();
			checkOutputsDir();
            System.err.println("namespaces checked + check shared volume + check output dirs");

            volume = new K8sVolume(conf, workflowName);
            volume.createPV();
            volume.createPVC();
            System.err.println("volumes created");

        } catch (Exception e) {
            logger.error("Failed to init the manager", e);
        }
    }

    public void destroy() {
        end = true;

        try {
            volume.deletePVC();
            volume.deletePV();
            
            /* hard cleaning not prod */
            for (K8sJob job : jobs) { job.clean(); }
            volume = null;
        } catch (ApiException e) {
            logger.error("Failed to destroy the manager");
        }
    }

    /*
     * Check if the k8s cluster already have the namespace
     * if it isn't here, then it is created
     */
    public void checkNamespace() throws ApiException {
        CoreV1Api api = K8sConfiguration.getInstance().getK8sCoreApi();
        V1NamespaceList list = api.listNamespace().execute();
        String targetName = K8sConfiguration.getInstance().getK8sNamespace();
        
        for (V1Namespace ns : list.getItems()) {
            String name = ns.getMetadata().getName();
            
            if (name.equals(targetName))
                return;
        }

        V1Namespace ns = new V1Namespace()
            .metadata(new V1ObjectMeta().name(targetName));

        api.createNamespace(ns).execute();
    }

	/**
	 * Check if the /workflows/sharedata volume exist 
	 */
	public void checkSharedVolume() throws ApiException {
		K8sVolume sharedUserVolume = K8sVolume.retrieve("SharedData");

		if (sharedUserVolume == null) {
			sharedUserVolume = new K8sVolume(K8sConfiguration.getInstance(), "SharedData");

			sharedUserVolume.createPV();
			sharedUserVolume.createPVC();
		}
		sharedVolume = sharedUserVolume;
	}

	/**
	 * Check for existance of STDOUR and STDERR from the plugin machine.
	 */
	public void checkOutputsDir() {
		File stdOutDir = new File(GaswConstants.OUT_ROOT);
        	if (!stdOutDir.exists()) {
            	stdOutDir.mkdirs();
        }
		File stdErrDir = new File(GaswConstants.ERR_ROOT);
        	if (!stdErrDir.exists()) {
            	stdErrDir.mkdirs();
        }
	}
q
    /**
     * Create a thread instance that will launch job when ressources are available (volumes)
     * The end variable is used to know if a thread instance has already been launched.
     * @param exec
     */
    private void submitter(K8sJob exec) {
        if (end == null) {
            end = false;
            new Thread(this.new K8sRunner()).start();
        }
        synchronized (this) {
            jobs.add(exec);
        }
    }

    public void submitter(List<String> cmd, String dockerImage, String jobID) {
        K8sJob exec = new K8sJob(jobID, cmd, dockerImage, volume, sharedVolume);
        submitter(exec);
    }

    class K8sRunner implements Runnable {
        private Boolean ready = false;
        private DateTime startedTime;

		@Override
        public void run() {
            try {
                startedTime = DateTime.now();
                loop();
            } catch (GaswException e) {
                logger.error(e.getMessage());
            } catch (Exception e) {
                logger.error("Something bad happened during the K8sRunner", e);
                logger.error(e.getStackTrace());
            }
        }

        private void loop() throws ApiException, InterruptedException, GaswException {
            while (ready == false) {
                Duration diff = new Duration(startedTime, DateTime.now());
                
                if (diff.getStandardSeconds() > 120)
                    throw new GaswException("Volume wasn't eady in 2 minutes, aborting !");
                else {
                    checker();
                    TimeUnit.MILLISECONDS.sleep(600);
                }
            }
            while (end == false) {
                synchronized (this) {
                    for (K8sJob exec : jobs) {
                        if (exec.getStatus() == GaswStatus.NOT_SUBMITTED) {
                            exec.start();
                        }
                    }
                }
                TimeUnit.MILLISECONDS.sleep(600);
            }
        }

        private synchronized void checker() {
            if (!ready && volume.isAvailable() && sharedVolume.isAvailable())
                ready = true;
        }
    }

    public ArrayList<K8sJob> getUnfinishedJobs() { 
        ArrayList<K8sJob> copy = new ArrayList<K8sJob>(jobs);

		System.err.println("voici les jobs non fini avant copi " + copy.toString());
        Iterator<K8sJob> it = copy.iterator();
        while (it.hasNext()) {
               if (it.next().isTerminated())
                   it.remove();
        }
        return copy; 
    }

    public K8sJob getJob(String jobId) {
        for (K8sJob j : jobs) {
            if (j.getJobID() == jobId)
                return j;
        }
        return null;
    } 
}
