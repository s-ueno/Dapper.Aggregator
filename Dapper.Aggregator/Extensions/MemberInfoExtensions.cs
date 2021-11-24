using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    public static class MemberInfoExtensions
    {
        internal static ColumnAttribute CreateColumnInfo(this MemberInfo pi)
        {
            var ret = new ColumnAttribute();
            ret.Name = pi.Name;
            ret.PropertyInfoName = pi.Name;

            var allAtts = pi.GetCustomAttributes(true).ToArray();
            var cInfo = allAtts.OfType<ColumnAttribute>().FirstOrDefault();
            if (cInfo != null)
            {
                cInfo.PropertyInfoName = pi.Name;
                return cInfo;
            }
            return ret;
        }
    }
}
