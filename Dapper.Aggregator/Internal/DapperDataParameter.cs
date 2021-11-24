using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    internal class DapperDataParameter
    {
        public string TargetName { get; private set; }
        PropertyAccessorImp acc;
        public DapperDataParameter(string target, PropertyAccessorImp pi)
        {
            this.TargetName = target;
            this.acc = pi;
        }

        public Criteria CreateIdCriteria(object obj, int index)
        {
            return new IdCriteria(acc.GetValue(obj), TargetName, index);
        }
    }
}
