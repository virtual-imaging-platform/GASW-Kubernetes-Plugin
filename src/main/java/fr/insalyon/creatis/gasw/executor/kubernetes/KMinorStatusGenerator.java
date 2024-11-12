package fr.insalyon.creatis.gasw.executor.kubernetes;

import fr.insalyon.creatis.gasw.execution.GaswMinorStatusServiceGenerator;
import lombok.NoArgsConstructor;

/**
 * @deprecated
 */
@NoArgsConstructor
public class KMinorStatusGenerator extends GaswMinorStatusServiceGenerator {

    @Override
    public String getClient() { return null; }

    @Override
    public String getServiceCall() { return null; }
}
