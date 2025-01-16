using Microsoft.Diagnostics.EventFlow;
using Microsoft.Extensions.Configuration;
using Validation;

namespace Mamemaki.EventFlow.Outputs.BigQuery.Storage
{
    public class BigQueryStorageOutputFactory : IPipelineItemFactory<BigQueryStorageOutput>
    {
        public BigQueryStorageOutput CreateItem(IConfiguration configuration, IHealthReporter healthReporter)
        {
            Requires.NotNull(configuration, nameof(configuration));
            Requires.NotNull(healthReporter, nameof(healthReporter));

            return new BigQueryStorageOutput(configuration, healthReporter);
        }
    }
}
