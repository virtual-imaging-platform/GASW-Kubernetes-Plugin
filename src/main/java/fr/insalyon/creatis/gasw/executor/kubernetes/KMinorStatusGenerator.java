package fr.insalyon.creatis.gasw.executor.kubernetes;

import fr.insalyon.creatis.gasw.execution.GaswMinorStatusServiceGenerator;
import lombok.NoArgsConstructor;

/**
 * @version DEPRECRATED CLASS
 */
@NoArgsConstructor
public class KMinorStatusGenerator extends GaswMinorStatusServiceGenerator {

    @Override
    public String getClient() { return null; }

    @Override
    public String getServiceCall() { return null; }
}
