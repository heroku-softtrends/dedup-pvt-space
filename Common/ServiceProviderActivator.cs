using Hangfire;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Common
{
    public class ServiceProviderActivator: JobActivator
    {
        private readonly IServiceProvider _serviceProvider;

        public ServiceProviderActivator(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;
        }

        public override object ActivateJob(Type type)
        {
            return _serviceProvider.GetService(type);
        }
    }
}
