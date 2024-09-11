package fr.insalyon.creatis.gasw.executor.kubernetes;

import fr.insalyon.creatis.gasw.execution.GaswMinorStatusServiceGenerator;

/**
 * @version DEPRECRATED CLASS
 */
public class K8sMinorStatusGenerator extends GaswMinorStatusServiceGenerator {

    @Override
    public String getClient() { return null; }

    @Override
    public String getServiceCall() { return null; }
}
