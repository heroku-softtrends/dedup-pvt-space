using System;

namespace Dedup.ViewModels
{
    public struct ResourceViewModel
    {
        public Guid uuid { get; set; }
        public string heroku_id { get; set; }
        public string plan { get; set; }
        public string region { get; set; }
        public string callback_url { get; set; }
        public dynamic options { get; set; }
        public string app_name { get; set; }
        public OauthGrant? oauth_grant { get; set; }
        public DateTime? created_at { get; set; }
        public DateTime? updated_at { get; set; }
    }
}
