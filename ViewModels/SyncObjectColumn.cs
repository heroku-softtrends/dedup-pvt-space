using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class SyncObjectColumn
    {
        private bool? _isRequired;
        private bool? _isPrimaryKey;

        public string name { get; set; }
        public string fieldType { get; set; }
		public int maxLength { get; set; }
        public int precision { get; set; }
        public bool? isRequired
        {
            get
            {
                return _isRequired.HasValue ? _isRequired : false;
            }
            set {
                _isRequired = value;
            }
        }
        public bool? isPrimaryKey
        {
            get
            {
                return _isPrimaryKey.HasValue ? _isPrimaryKey : false;
            }
            set
            {
                _isPrimaryKey = value;
            }
        }
        public string defaultValue { get; set; }
    }
}
