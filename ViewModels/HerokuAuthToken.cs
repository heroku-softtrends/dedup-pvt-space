using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct HerokuAuthToken
    {
        public string auth_id { get; set; }
        public string access_token { get; set; }
        public int expires_in { get; set; }
        public string refresh_token { get; set; }
        public string token_type { get; set; }
        public string user_id { get; set; }
        public string session_nonce { get; set; }
    }
}
