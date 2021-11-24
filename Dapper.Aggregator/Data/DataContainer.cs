using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Aggregator
{
    [Serializable]
    public class DataContainer
    {
        internal DataContainer() { }
        internal DataContainer(object current, RelationAttribute[] atts, DataStore dataStore)
        {
            Current = current;
            Atts = atts;
            DataStore = dataStore;
        }
        protected object Current { get; private set; }
        protected RelationAttribute[] Atts { get; private set; }
        internal protected DataStore DataStore { get; private set; }
        public virtual IEnumerable<T> GetChildren<T>(string key = null)
        {
            var t = typeof(T);
            RelationAttribute att;
            if (!string.IsNullOrWhiteSpace(key))
            {
                att = Atts.FirstOrDefault(x => x.Key == key);
                if (att == null)
                    throw new KeyNotFoundException(key + " is not found in RelationAttribute");
            }
            else
            {
                att = Atts.FirstOrDefault(x => x.ChildType == t);
                if (att == null)
                {
                    var newT = ILGeneratorUtil.InjectionInterfaceWithProperty(typeof(T));
                    att = Atts.FirstOrDefault(x => x.ChildType == newT);
                }
                if (att == null)
                    throw new KeyNotFoundException(typeof(T).Name + " is not found in RelationAttribute");

                key = att.Key;
            }
            var rows = DataStore.Find(Current, att);
            return rows.Cast<T>();
        }
    }

}
