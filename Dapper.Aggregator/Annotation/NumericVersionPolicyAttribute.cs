using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    [Serializable]
    public class NumericVersionPolicyAttribute : VersionPolicyAttribute
    {
        public override object Generate(object currentVersionValue)
        {
            var lockVersion = currentVersionValue as int?;
            return lockVersion.HasValue ? lockVersion.Value + 1 : 0;
        }
    }
}
