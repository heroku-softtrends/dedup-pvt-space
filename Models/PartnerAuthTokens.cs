using Dedup.Common;
using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Dedup.Models
{
    public class PartnerAuthTokens
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string auth_id { get; set; }

        public string oauth_code { get; set; }

        public AuthGrantType oauth_type { get; set; }

        public Nullable<DateTime> oauth_expired_in { get; set; }

        public string access_token { get; set; }

        public string refresh_token { get; set; }

        public string token_type { get; set; }

        public Nullable<DateTime> expires_in { get; set; }

        public string user_id { get; set; }

        public string session_nonce { get; set; }

        public string redirect_url { get; set; }

        public DateTime created_at { get; set; }

        public DateTime updated_at { get; set; }

        [ForeignKey("auth_id")]
        public virtual Resources Resource { get; set; }
    }
}
