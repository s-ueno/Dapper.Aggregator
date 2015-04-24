using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Dapper.Aggregater
{
    public static class DapperExtensions
    {
        public static IEnumerable<T> QueryWith<T>(this IDbConnection cnn, string sql, object param = null,
            IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null, int splitLength = 300) where T : IContainerHolder
        {
            var command = new CommandDefinition(sql, param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None);
            return QueryWith<T>(cnn, command, splitLength);
        }
        public static IEnumerable<T> QueryWith<T>(this IDbConnection cnn, CommandDefinition command, int splitLength = 300) where T : IContainerHolder
        {
            var atts = typeof(T).GetCustomAttributes(typeof(RelationAttribute), true).OfType<RelationAttribute>().ToArray();
            if (!atts.Any())
                throw new InvalidOperationException("QueryWith is function to use DataRelationAttribute.");

            var rows = cnn.Query<T>(command);
            if (rows == null || !rows.Any()) return new T[] { };

            var rootRecordCount = 0;
            var list = rows as ICollection<T>;
            if (list != null)
            {
                rootRecordCount = list.Count;
            }
            else
            {
                rootRecordCount = rows.Count();
            }

            foreach (var each in atts)
            {
                if (each.ParentType == null)
                    each.ParentType = typeof(T);
                each.Ensure();
                each.DataAdapter.SplitCount = splitLength;
            }

            LoadWith(cnn, command, typeof(T), atts, rows);

            return rows;
        }

        private static void LoadWith(IDbConnection cnn, CommandDefinition command, Type t, RelationAttribute[] atts, System.Collections.IEnumerable roots)
        {
            var rootAtts = atts.Where(x => !x.Loaded && x.ParentType == t).ToArray();
            if (!rootAtts.Any()) return;

            var rootDataStore = new DataStore();
            var enumerator = roots.GetEnumerator();
            var hasValue = enumerator.MoveNext();
            while (hasValue)
            {
                var value = enumerator.Current;
                var holder = value as IContainerHolder;
                if (holder != null)
                {
                    holder.Container = new DataContainer(value, rootAtts, rootDataStore);
                }
                foreach (var att in rootAtts)
                {
                    att.DataAdapter.AssignDataParameter(value);
                }
                hasValue = enumerator.MoveNext();
            }

            foreach (var att in rootAtts)
            {
                var list = att.DataAdapter.Fill(cnn, command);
                att.Loaded = true;
                rootDataStore.Add(att.Key, list);
                if (list.Count != 0)
                {
                    LoadWith(cnn, command, att.ChildType, atts, list);
                }
            }
        }
    }

    internal class DapperDataAdapter
    {
        public int SplitCount { get; set; }

        RelationAttribute relationAttribute;
        PropertyInfo[] parentPropertyInfo;
        List<DapperDataParameter> dataParameter = new List<DapperDataParameter>();
        public DapperDataAdapter(RelationAttribute att)
        {
            this.relationAttribute = att;
            parentPropertyInfo = att.ParentType.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).ToArray();

            for (int i = 0; i < att.ChildPropertyNames.Length; i++)
            {
                var target = att.ChildPropertyNames[i];
                var pi = att.parentPropertyAccessors[i];
                dataParameter.Add(new DapperDataParameter(target, pi));
            }
        }

        List<Criteria> childCriteriaList = new List<Criteria>();
        public void AssignDataParameter(object value)
        {
            var list = new List<Criteria>();
            foreach (var each in dataParameter)
            {
                list.Add(each.CreateIdCriteria(value));
            }

            if (list.Count == 1)
            {
                childCriteriaList.Add(list.First());
            }
            else
            {
                childCriteriaList.Add(new OperatorCriteria("AND", list.ToArray()));
            }
        }

        public List<object> Fill(IDbConnection cnn, CommandDefinition command)
        {
            var result = new List<object>();
            var tableType = relationAttribute.ChildType;
            var tableName = relationAttribute.ChildTableName;
            var splitCriteria = SplitCriteria();
            foreach (var each in splitCriteria)
            {
                var statement = each.BuildStatement();
                var param = each.BuildParameters();
                var sql = string.Format("select * from {0} where {1} ", tableName, statement);
                var rows = cnn.Query(tableType, sql, param, command.Transaction, command.Buffered, command.CommandTimeout, command.CommandType);

                result.AddRange(rows);
            }
            return result;
        }
        private IEnumerable<Criteria> SplitCriteria()
        {
            var result = new List<Criteria>();
            var criteriaList = childCriteriaList.Distinct().ToList();

            if (criteriaList.Count < SplitCount)
            {
                result.Add(new OperatorCriteria("OR", criteriaList.ToArray()));
            }
            else
            {
                var buff = new List<Criteria>();
                for (int i = 0; i < criteriaList.Count; i++)
                {
                    buff.Add(criteriaList[i]);
                    if (i % SplitCount == 0)
                    {
                        result.Add(new OperatorCriteria("OR", buff.ToArray()));
                        buff.Clear();
                    }
                }
                if (buff.Count != 0)
                {
                    result.Add(new OperatorCriteria("OR", buff.ToArray()));
                }
            }
            return result;
        }
    }


    #region not use reflection. simple GetGetMethod access

    internal abstract class PropertyAccessorImp
    {
        public abstract object GetValue(object obj);
        public static PropertyAccessorImp ToAccessor(PropertyInfo pi)
        {
            var getterDelegateType = typeof(Func<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            var getter = Delegate.CreateDelegate(getterDelegateType, pi.GetGetMethod(true));
            var accessorType = typeof(PropertyInfoProvider<,>).MakeGenericType(pi.DeclaringType, pi.PropertyType);
            var provider = (PropertyAccessorImp)Activator.CreateInstance(accessorType, getter);
            return provider;
        }
    }
    internal class PropertyInfoProvider<TTarget, TProperty> : PropertyAccessorImp
    {
        private readonly Func<TTarget, TProperty> getter;
        public PropertyInfoProvider(Func<TTarget, TProperty> getter)
        {
            this.getter = getter;
        }
        public override object GetValue(object obj)
        {
            return this.getter((TTarget)obj);
        }
    }

    #endregion

    internal class DapperDataParameter
    {
        public string TargetName { get; private set; }
        PropertyAccessorImp acc;
        int index;
        public DapperDataParameter(string target, PropertyAccessorImp pi)
        {
            this.TargetName = target;
            this.acc = pi;
        }

        public Criteria CreateIdCriteria(object obj)
        {
            return new IdCriteria(acc.GetValue(obj), TargetName, ++index);
        }
    }

    [Serializable]
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = true)]
    public class RelationAttribute : Attribute
    {
        public RelationAttribute(Type childType, string parentPropertyName, string childPropertyName)
            : this(childType, string.Empty, parentPropertyName, childPropertyName) { }

        public RelationAttribute(Type childType, string key, string parentPropertyName, string childPropertyName)
            : this(childType, key, new string[] { parentPropertyName }, new string[] { childPropertyName }) { }

        public RelationAttribute(Type childType, string[] parentPropertyName, string[] childPropertyName)
            : this(childType, string.Empty, parentPropertyName, childPropertyName) { }

        public RelationAttribute(Type childType, string key, string[] parentPropertyName, string[] childPropertyName)
            : this(null, childType, key, parentPropertyName, childPropertyName) { }

        public RelationAttribute(Type parentType, Type childType, string parentPropertyName, string childPropertyName)
            : this(parentType, childType, string.Empty, new[] { parentPropertyName }, new[] { childPropertyName }) { }

        public RelationAttribute(Type parentType, Type childType, string[] parentPropertyName, string[] childPropertyName)
            : this(parentType, childType, string.Empty, parentPropertyName, childPropertyName) { }

        public RelationAttribute(Type parentType, Type childType, string key, string[] parentPropertyName, string[] childPropertyName)
        {
            ParentType = parentType;
            ChildType = childType;
            ParentPropertyNames = parentPropertyName;
            ChildPropertyNames = childPropertyName;
            Key = key;
        }
        public Type ParentType { get; internal set; }
        public Type ChildType { get; private set; }
        public string[] ParentPropertyNames { get; private set; }
        public string[] ChildPropertyNames { get; private set; }
        public string Key { get; private set; }
        public string ParentTableName { get; private set; }
        public string ChildTableName { get; private set; }

        [NonSerialized]
        internal List<PropertyAccessorImp> parentPropertyAccessors = new List<PropertyAccessorImp>();
        [NonSerialized]
        internal List<PropertyAccessorImp> childPropertyAccessors = new List<PropertyAccessorImp>();

        internal bool Loaded { get; set; }
        public void Ensure()
        {
            if (string.IsNullOrWhiteSpace(Key))
            {
                Key = CreateDefaultKey(ParentType, ChildType);
            }
            ParentTableName = GetTableName(ParentType);
            ChildTableName = GetTableName(ChildType);

            var parentProperties = ParentType.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).ToArray();
            var childProperties = ChildType.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).ToArray();
            for (int i = 0; i < ParentPropertyNames.Length; i++)
            {
                var pName = ParentPropertyNames[i];
                var cName = ChildPropertyNames[i];

                var ppi = parentProperties.Single(x => x.Name == pName);
                var cpi = childProperties.Single(x => x.Name == cName);

                parentPropertyAccessors.Add(PropertyAccessorImp.ToAccessor(ppi));
                childPropertyAccessors.Add(PropertyAccessorImp.ToAccessor(cpi));
            }

            DataAdapter = new DapperDataAdapter(this);
        }

        [NonSerialized]
        internal DapperDataAdapter DataAdapter;
        static string CreateDefaultKey(Type parentType, Type childType)
        {
            return string.Format("{0}.{1}", parentType.Name, childType.Name);
        }
        //Dapper.Contrib
        static string GetTableName(Type type)
        {
            string name;
            if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
            {
                name = type.Name + "s";
                if (type.IsInterface && name.StartsWith("I"))
                    name = name.Substring(1);

                //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableattr = type.GetCustomAttributes(false).Where(attr => attr.GetType().Name == "TableAttribute").SingleOrDefault() as dynamic;
                if (tableattr != null)
                    name = tableattr.Name;
                TypeTableName[type.TypeHandle] = name;
            }
            return name;
        }

        [NonSerialized]
        static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();
    }

    public interface IContainerHolder
    {
        DataContainer Container { get; set; }
    }

    [Serializable]
    public class DataStore
    {
        private Dictionary<string, List<object>> dic = new Dictionary<string, List<object>>();
        internal void Add(string key, List<object> rows)
        {
            dic[key] = rows;
        }

        internal IEnumerable<object> Find(object parent, string key, RelationAttribute att)
        {
            //http://stackoverflow.com/questions/7458139/net-is-type-gethashcode-guaranteed-to-be-unique
            //hack sorry if not unique. hahaha
            var hash = parent.GetHashCode() ^ key.GetHashCode();
            if (hashDic.ContainsKey(hash))
                return hashDic[hash];

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
        static ConcurrentDictionary<int, List<object>> hashDic = new ConcurrentDictionary<int, List<object>>();
        private class ValueEqualityComparer : IEqualityComparer<object>
        {
            public bool Equals(object x, object y)
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
        protected DataStore DataStore { get; private set; }
        public IEnumerable<T> GetChildren<T>(string key = null)
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
                    throw new KeyNotFoundException(typeof(T).Name + " is not found in RelationAttribute");

                key = att.Key;
            }
            var rows = DataStore.Find(Current, key, att);
            return rows.Cast<T>();
        }
    }



    #region Criteria Pattern

    public abstract class Criteria
    {
        public abstract Dictionary<string, object> BuildParameters();
        public abstract string BuildStatement();

        public static Criteria operator &(Criteria c1, Criteria c2)
        {
            return new OperatorCriteria("AND", c1, c2);
        }
        public static Criteria operator |(Criteria c1, Criteria c2)
        {
            return new OperatorCriteria("OR", c1, c2);
        }
    }

    public class OperatorCriteria : Criteria, IEquatable<OperatorCriteria>
    {
        public Criteria[] Args { get; private set; }
        public string Operator { get; private set; }
        public OperatorCriteria(string @operator, params Criteria[] args)
        {
            Operator = @operator;
            Args = args;
        }
        public override string BuildStatement()
        {
            if (Args == null || !Args.Any()) return null;
            if (Args.Length == 1)
                return Args.First().BuildStatement();

            var list = new List<string>();
            foreach (var each in Args)
            {
                var statement = each.BuildStatement();
                if (!string.IsNullOrWhiteSpace(statement))
                    list.Add(string.Format("({0})", statement));
            }
            return string.Join(" " + Operator + " ", list);
        }

        public override int GetHashCode()
        {
            var hash = 0;
            hash = Operator.GetHashCode();
            foreach (var each in Args)
            {
                hash = hash ^ each.GetHashCode();
            }
            return hash;
        }
        public override bool Equals(object obj)
        {
            return (this as IEquatable<OperatorCriteria>).Equals(obj as OperatorCriteria);
        }
        bool IEquatable<OperatorCriteria>.Equals(OperatorCriteria other)
        {
            if (other == null) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return Operator == other.Operator && Args.SequenceEqual(other.Args);
        }

        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            foreach (var each in Args)
            {
                var parameters = each.BuildParameters();
                foreach (var kp in parameters)
                {
                    dic.Add(kp.Key, kp.Value);
                }
            }
            return dic;
        }
    }

    public class IdCriteria : Criteria, IEquatable<IdCriteria>
    {
        public IdCriteria(object value, string name, int index = 0)
        {
            Value = value;
            Name = name;
            Index = index;
        }
        public object Value { get; private set; }
        public string Name { get; private set; }
        public int Index { get; private set; }

        public override string BuildStatement()
        {
            return string.Format("{0} = @p{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic.Add(string.Format("@p{0}", Index), Value);
            return dic;
        }
        public override int GetHashCode()
        {
            var hash = 0;
            if (Value != null)
            {
                hash = Value.GetHashCode();
            }
            hash = hash ^ Name.GetHashCode();
            return hash;
        }
        public override bool Equals(object obj)
        {
            return (this as IEquatable<IdCriteria>).Equals(obj as IdCriteria);
        }
        bool IEquatable<IdCriteria>.Equals(IdCriteria other)
        {
            if (other == null) return false;
            if (object.ReferenceEquals(this, other)) return true;

            if (Value == null)
            {
                return other.Value == null && Name == other.Name;
            }

            return Value.Equals(other.Value) && Name == other.Name;
        }
    }

    public static class CriteriaExtensionMethods
    {
        public static Criteria And(this Criteria criteria, Criteria otherCriteria)
        {
            return new OperatorCriteria("AND", criteria, otherCriteria);
        }
        public static Criteria Or(this Criteria criteria, Criteria otherCriteria)
        {
            return new OperatorCriteria("OR", criteria, otherCriteria);
        }
    }

    #endregion

}
