using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    [Serializable]
    public abstract class VersionPolicyAttribute : Attribute
    {
        public abstract object Generate(object currentVersionValue);
    }

    internal class DefalutVersionPolicy : VersionPolicyAttribute
    {
        public override object Generate(object currentVersionValue)
        {
            if (currentVersionValue is int i)
            {
                return ++i;
            }
            if (currentVersionValue is long l)
            {
                return ++l;
            }
            if (currentVersionValue is double d)
            {
                return ++d;
            }
            if (decimal.TryParse(currentVersionValue.ToString(), out decimal p))
            {
                return ++p;
            }
            return currentVersionValue;
        }
    }
}
