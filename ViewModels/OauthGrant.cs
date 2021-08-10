using Dedup.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct OauthGrant
    {
        public string code { get; set; }
        public AuthGrantType type { get; set; }
        public DateTime expires_at { get; set; }
    }
}
