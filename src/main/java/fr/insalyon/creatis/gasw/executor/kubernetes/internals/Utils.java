package fr.insalyon.creatis.gasw.executor.kubernetes.internals;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j;

@Log4j @NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Utils {

    public static void sleepNException(final long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            log.trace("SleepNException interrupted !");
        }
    }
}
