using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class AppViewModel
    {
        public string resourceId { get; set; }
        public string herokuAuthToken { get; set; }
        public string envVarName { get; set; }
    }
}
