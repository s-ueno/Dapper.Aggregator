using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    public enum PersistState
    {
        Insert,
        Update,
        Delete,
    }

    [Serializable]
    public abstract class EntityValidateAttribute : Attribute
    {
        internal Exception GetError(object entity, PersistState persistState)
        {
            Exception ret = null;
            try
            {
                Valid(entity, persistState);
            }
            catch (Exception ex)
            {
                ret = new Exception("Validation Error.", ex);
            }
            return ret;
        }
        public abstract void Valid(object entity, PersistState persistState);
    }   
}
