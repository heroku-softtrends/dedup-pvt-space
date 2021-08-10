using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class ErrorViewModel
    {
        public HttpStatusCode Code { get; set; }

        public string Message { get; set; }
    }
}
