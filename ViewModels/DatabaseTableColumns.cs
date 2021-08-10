using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public struct DatabaseTableColumns
    {
        private bool? _isRequired;
        private bool? _isPrimaryKey;
        private bool? _isUniqueKey;

        public string dbSchema { get; set; }
        public string tableName { get; set; }
        public string name { get; set; }
        public string fieldType { get; set; }
		public int length { get; set; }
        public bool? isRequired
        {
            get
            {
                return _isRequired.HasValue ? _isRequired : false;
            }
            set
            {
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
        public bool? isUniqueKey
        {
            get
            {
                return _isUniqueKey.HasValue ? _isUniqueKey : false;
            }
            set
            {
                _isUniqueKey = value;
            }
        }
        public string defaultValue { get; set; }
    }
}
