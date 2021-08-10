using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Dedup.ViewModels
{
    public class PGQueryStatus : IDisposable
    {
        public int total_rows { set; get; }
        public int inserted_rows { set; get; }
        public int updated_rows { set; get; }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private bool _disposed = false;
        public virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                //if (disposing)
                //{
                //}
                //else
                //{
                //}
                _disposed = true;
            }
        }

        ~PGQueryStatus()
        {
            Dispose(false);
        }
    }
}
