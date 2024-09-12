package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

public class Utils {
    public static void sleepNException(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {}
    }
}
