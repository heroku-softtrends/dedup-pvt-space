using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Dedup.Models
{
    public class Resources
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string uuid { get; set; }

        [DataType(DataType.Text)]
        public string heroku_id { get; set; }

        [DataType(DataType.Text)]
        public string plan { get; set; }

        [DataType(DataType.Text)]
        public string region { get; set; }

        [DataType(DataType.Text)]
        public string callback_url { get; set; }

        [DataType(DataType.Text)]
        public string app_name { get; set; }

        [DataType(DataType.Text)]
        public string private_app_url { get; set; }

        [DataType(DataType.DateTime)]
        public DateTime updated_at { get; set; }

        [DataType(DataType.DateTime)]
        public DateTime created_at { get; set; }

        [DataType(DataType.DateTime)]
        public DateTime? expired_at { get; set; }

        [DataType(DataType.Text)]
        public string user_organization { get; set; }

        [DataType(DataType.Text)]
        public string user_name { get; set; }

        [DataType(DataType.Text)]
        public string user_email { get; set; }
		
		public bool? is_license_accepted { get; set; }
				
        public virtual PartnerAuthTokens partnerAuthToken { get; set; }
        public virtual DeDupSettings DedupSetting { get; set; }
        public virtual AuthTokens AuthToken { get; set; }
    }
}