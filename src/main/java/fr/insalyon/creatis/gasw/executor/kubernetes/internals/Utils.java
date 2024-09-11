package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConstants;

public class Utils {
    public static void sleepNException(long time) {
        try {
            Thread.sleep(K8sConstants.statusRetryWait);
        } catch (InterruptedException e) {}
    }
}
