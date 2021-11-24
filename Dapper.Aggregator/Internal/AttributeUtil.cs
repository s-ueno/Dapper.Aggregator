using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    //Cache
    internal static class AttributeUtil
    {
        internal static IEnumerable<T> Find<T>(Type t) where T : Attribute
        {
            var list = new List<Attribute>();
            if (!dic.TryGetValue(t, out list))
            {
                dic[t] = list = t.GetCustomAttributes(true).OfType<Attribute>().ToList();
            }
            return list.OfType<T>();
        }
        private static ConcurrentDictionary<Type, List<Attribute>> dic = new ConcurrentDictionary<Type, List<Attribute>>();
    }
}
