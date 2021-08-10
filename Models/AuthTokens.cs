using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Models
{
    public class AuthTokens
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string auth_id { get; set; }
        [Required]
        public string access_token { get; set; }
        [Required]
        public string refresh_token { get; set; }
        public string token_type { get; set; }
        [Required]
        public DateTime expires_in { get; set; }
        public string user_id { get; set; }
        public string session_nonce { get; set; }
        public string redirect_url { get; set; }
        public DateTime created_at { get; set; }
        public DateTime updated_at { get; set; }

        [ForeignKey("auth_id")]
        public virtual Resources Resource { get; set; }
    }
}
