package fr.insalyon.creatis.gasw.executor.kubernetes;

import fr.insalyon.creatis.gasw.GaswConfiguration;
import fr.insalyon.creatis.gasw.GaswException;
import fr.insalyon.creatis.gasw.GaswInput;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConfiguration;
import fr.insalyon.creatis.gasw.executor.kubernetes.config.K8sConstants;
import fr.insalyon.creatis.gasw.executor.kubernetes.internals.K8sManager;
import fr.insalyon.creatis.gasw.plugin.ExecutorPlugin;
import java.util.ArrayList;
import java.util.List;
import net.xeoh.plugins.base.annotations.PluginImplementation;

@PluginImplementation
public class K8sExecutor implements ExecutorPlugin {

    private K8sSubmit k8sSubmit;
    private K8sManager manager;

    @Override
    public String getName() {
        return K8sConstants.EXECUTOR_NAME;
    }

    @Override
    public void load(GaswInput gaswInput) throws GaswException {
        K8sConfiguration conf = K8sConfiguration.getInstance();

        conf.init(K8sConstants.pluginConfig);
        manager = new K8sManager(GaswConfiguration.getInstance().getSimulationID());
        manager.init();
        K8sMonitor.getInstance().setManager(manager);
        
        k8sSubmit = new K8sSubmit(gaswInput, new K8sMinorStatusGenerator(), manager);
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
        K8sMonitor.getInstance().finish();
    }
}
