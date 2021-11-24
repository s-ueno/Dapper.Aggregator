using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    public static class TypeExtensions
    {
        //Dapper.Contrib
        public static string GetTableName(this Type type)
        {
            string name;
            if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
            {
                name = type.Name;
                if (type.IsInterface && name.StartsWith("I"))
                    name = name.Substring(1);

                //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableattr = type.GetCustomAttributes<TableAttribute>(true).SingleOrDefault();
                if (tableattr != null)
                {
                    name = tableattr.Name;
                }
                TypeTableName[type.TypeHandle] = name;
            }
            return name;
        }
        static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();




        public static ColumnInfoCollection GetSelectClause(this Type type)
        {
            ColumnInfoCollection clause = null;
            if (!TypeSelectClause.TryGetValue(type.TypeHandle, out clause))
            {
                clause = new ColumnInfoCollection();
                var props = type.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).ToArray();
                foreach (var each in props)
                {
                    if (each.PropertyType == typeof(DataContainer))
                        continue;
                    if (!each.CanWrite)
                        continue;
                    clause.Add(each.CreateColumnInfo());
                }
            }
            return clause;
        }
        static readonly ConcurrentDictionary<RuntimeTypeHandle, ColumnInfoCollection> TypeSelectClause
            = new ConcurrentDictionary<RuntimeTypeHandle, ColumnInfoCollection>();



        public static IEnumerable<IndexAttribute> GetIndexes(this Type type)
        {
            if (!Indexes.TryGetValue(type.TypeHandle, out var indexes))
            {
                indexes = type.GetCustomAttributes<IndexAttribute>(true).ToArray();
                var columns = type.GetSelectClause().ToDictionary(x => x.PropertyInfoName);
                foreach (var index in indexes)
                {
                    index.Columns = index.Columns.Select(x => columns[x].Name).ToArray();
                }
                Indexes[type.TypeHandle] = indexes;
            }
            return indexes;
        }
        static readonly ConcurrentDictionary<RuntimeTypeHandle, IndexAttribute[]> Indexes = new ConcurrentDictionary<RuntimeTypeHandle, IndexAttribute[]>();



    }
}
