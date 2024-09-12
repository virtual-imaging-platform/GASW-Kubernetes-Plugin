package fr.insalyon.creatis.gasw.executor.kubernetes;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.KConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.KManager;
import fr.insalyon.creatis.gasw.plugin.ExecutorPlugin;
import java.util.ArrayList;
import java.util.List;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
public class KExecutor implements ExecutorPlugin {

    private KSubmit   k8sSubmit;
    private KManager  manager;
    private boolean     loaded = false;

    @Override
    public String getName() {
        return KConstants.EXECUTOR_NAME;
    }

    @Override
    public void load(GaswInput gaswInput) throws GaswException {
        if ( ! loaded) {
            KConfiguration conf = KConfiguration.getInstance();

            conf.init(KConstants.pluginConfig);
            manager = new KManager(GaswConfiguration.getInstance().getSimulationID());
            manager.init();

            KMonitor.getInstance().setManager(manager);
            loaded = true;
        }
        
        k8sSubmit = new KSubmit(gaswInput, new KMinorStatusGenerator(), manager);
    }

    @Override
    public List<Class> getPersistentClasses() throws GaswException {
        return new ArrayList<Class>();
    }

    @Override
    public String submit() throws GaswException {
        return k8sSubmit.submit();
    }

    @Override
    public void terminate() throws GaswException {
        // Plugin
        manager.destroy();

        // Gasw
        KMonitor.getInstance().finish();
    }
}
