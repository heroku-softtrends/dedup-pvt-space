using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Dedup.Models
{
    public class DeDupSettings
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public string ccid { get; set; }

        public string database_config_json { get; set; }

        public DateTime created_at { get; set; }

        public DateTime updated_at { get; set; }

        [ForeignKey("ccid")]
        public virtual Resources Resource { get; set; }

        public virtual ICollection<Connectors> Connectors { get; set; }
    }
}
