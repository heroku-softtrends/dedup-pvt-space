using Dedup.ViewModels;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.Common
{
    public class ObjectComparer : IEqualityComparer<SyncObjectColumn>
    {
        public bool Equals(SyncObjectColumn table1, SyncObjectColumn table2)
        {
            if (table1.name != table2.name || (table1.name == table2.name && table1.fieldType != table2.fieldType))
            {
                return true;
            }
            return false;
        }

        bool IEqualityComparer<SyncObjectColumn>.Equals(SyncObjectColumn x, SyncObjectColumn y)
        {
            throw new NotImplementedException();
        }

        int IEqualityComparer<SyncObjectColumn>.GetHashCode(SyncObjectColumn obj)
        {
            throw new NotImplementedException();
        }
    }
}
