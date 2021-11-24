using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

namespace Dapper.Aggregator
{
    [Serializable]
    public class DataStore
    {
        private Dictionary<string, List<object>> dic = new Dictionary<string, List<object>>();
        internal void Add(string key, List<object> rows)
        {
            dic[key] = rows;
        }

        internal IEnumerable<object> Find(object parent, RelationAttribute att)
        {
            //http://stackoverflow.com/questions/7458139/net-is-type-gethashcode-guaranteed-to-be-unique
            //hack 
            var key = att.Key;
            var hash = parent.GetHashCode() ^ key.GetHashCode();

            if (hashDic == null)
                hashDic = new ConcurrentDictionary<int, List<object>>();

            if (hashDic.ContainsKey(hash))
                return hashDic[hash];

            if (!dic.ContainsKey(key)) return null;

            hashDic[hash] = new List<object>();

            var matchImtes = new List<object>();
            var keys = new List<object>();
            foreach (var each in att.parentPropertyAccessors)
            {
                keys.Add(each.GetValue(parent));
            }

            var comparer = new ValueEqualityComparer();
            foreach (var each in dic[key])
            {
                var childList = new List<object>();
                foreach (var pi in att.childPropertyAccessors)
                {
                    childList.Add(pi.GetValue(each));
                }

                if (keys.SequenceEqual(childList, comparer))
                {
                    hashDic[hash].Add(each);
                }
            }
            return hashDic[hash];
        }

        [NonSerialized]
        ConcurrentDictionary<int, List<object>> hashDic = new ConcurrentDictionary<int, List<object>>();
        private class ValueEqualityComparer : IEqualityComparer<object>
        {
            new public bool Equals(object x, object y)
            {
                if (x == null)
                {
                    return y == null;
                }
                return x.Equals(y);
            }

            public int GetHashCode(object obj)
            {
                return obj.GetHashCode();
            }
        }
    }

}
