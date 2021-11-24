using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    [Serializable]
    public abstract class EntityValidateAttribute : Attribute
    {
        internal Exception GetError(object entity)
        {
            Exception ret = null;
            try
            {
                Valid(entity);
            }
            catch (Exception ex)
            {
                ret = new Exception("Validation Error.", ex);
            }
            return ret;
        }
        public abstract void Valid(object entity);
    }
}
