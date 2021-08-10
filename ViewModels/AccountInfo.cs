
using System;

namespace Dedup.ViewModels
{
    public struct AccountInfo
    {
        public bool allow_tracking { get; set; }
        public bool beta { get; set; }
        public string created_at { get; set; }
        public string email { get; set; }
        public bool federated { get; set; }
        public string id { get; set; }
        public AccountIdentityProvider? identity_provider { get; set; }
        public string last_login { get; set; }
        public string name { get; set; }
        public string sms_number { get; set; }
        public string suspended_at { get; set; }
        public string delinquent_at { get; set; }
        public bool two_factor_authentication { get; set; }
        public string updated_at { get; set; }
        public bool verified { get; set; }
        public AppOrganization? default_organization { get; set; }
    }
}
