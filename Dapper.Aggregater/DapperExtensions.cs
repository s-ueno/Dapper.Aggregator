﻿using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;

namespace Dapper.Aggregater
{
    public static class DapperExtensions
    {
        //poco pattern
        // If the class does not implement interface(IContainerHolder), I embed interface dynamically using TypeBuilder.
        public static IEnumerable<T> QueryWith<T>(this IDbConnection cnn, Query<T> query,
            IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null, int splitLength = 100, int queryOptimizerLevel = 10)
        {
            foreach (var each in query.Relations)
            {
                if (each.ParentType == null)
                    each.ParentType = typeof(T);

                each.Ensure();
                each.EnsureDynamicType();
                each.DataAdapter.SplitCount = splitLength;
                each.DataAdapter.QueryOptimizerLevel = queryOptimizerLevel;
            }

            var oldType = typeof(T);
            var newParentType = ILGeneratorUtil.IsInjected(oldType) ? ILGeneratorUtil.InjectionInterfaceWithProperty(oldType) : oldType;

            var rows = cnn.Query(newParentType, query.Sql, query.Parameters, transaction, buffered, commandTimeout, commandType);
            if (rows == null || !rows.Any()) return new T[] { };

            var command = new CommandDefinition(query.SqlIgnoreOrderBy, query.Parameters, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None);
            LoadWith(cnn, command, newParentType, query.Relations.ToArray(), rows);
            return rows.Cast<T>();
        }


        ////Implement IContainerHolder Interface pattern
        //// all typesafe.
        //public static IEnumerable<T> QueryWith<T>(this IDbConnection cnn, string sql, object param = null,
        //    IDbTransaction transaction = null, bool buffered = true, int? commandTimeout = null, CommandType? commandType = null, int splitLength = 100, int queryOptimizerLevel = 100) where T : IContainerHolder
        //{
        //    var command = new CommandDefinition(sql, param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None);
        //    return QueryWith<T>(cnn, command, splitLength, queryOptimizerLevel);
        //}
        //public static IEnumerable<T> QueryWith<T>(this IDbConnection cnn, CommandDefinition command, int splitLength = 100, int queryOptimizerLevel = 100) where T : IContainerHolder
        //{
        //    var atts = typeof(T).GetCustomAttributes(typeof(RelationAttribute), true).OfType<RelationAttribute>().ToArray();
        //    if (!atts.Any())
        //        throw new InvalidOperationException("QueryWith is function to use DataRelationAttribute.");

        //    var rows = cnn.Query<T>(command);
        //    if (rows == null || !rows.Any()) return new T[] { };

        //    foreach (var each in atts)
        //    {
        //        if (each.ParentType == null)
        //            each.ParentType = typeof(T);

        //        each.Ensure();
        //        each.DataAdapter.SplitCount = splitLength;
        //        each.DataAdapter.QueryOptimizerLevel = queryOptimizerLevel;
        //    }

        //    LoadWith(cnn, command, typeof(T), atts, rows);

        //    return rows;
        //}

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
                var list = att.DataAdapter.Fill(cnn, command, atts);
                att.Loaded = true;
                rootDataStore.Add(att.Key, list);
                if (list.Count != 0)
                {
                    LoadWith(cnn, command, att.ChildType, atts, list);
                }
            }
        }


        //Dapper.Contrib
        internal static string GetTableName(this Type type)
        {
            string name;
            if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
            {
                name = type.Name;
                if (type.IsInterface && name.StartsWith("I"))
                    name = name.Substring(1);

                //NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
                var tableattr = type.GetCustomAttributes(false).Where(attr => attr.GetType().Name == "TableAttribute").SingleOrDefault() as dynamic;
                if (tableattr != null)
                {
                    name = tableattr.Name;
                }
                else
                {
                    tableattr = type.GetCustomAttributes(true).Where(attr => attr.GetType().Name == "TableAttribute").SingleOrDefault() as dynamic;
                    if (tableattr != null)
                        name = tableattr.Name;
                }
                TypeTableName[type.TypeHandle] = name;
            }
            return name;
        }
        static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();

        internal static ColumnInfoCollection GetSelectClause(this Type type)
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
                    clause.Add(CreateColumnInfo(each));
                }
            }
            return clause;
        }
        static readonly ConcurrentDictionary<RuntimeTypeHandle, ColumnInfoCollection> TypeSelectClause
            = new ConcurrentDictionary<RuntimeTypeHandle, ColumnInfoCollection>();
        internal static ColumnInfoAttribute CreateColumnInfo(this MemberInfo pi)
        {
            var ret = new ColumnInfoAttribute();
            ret.Name = pi.Name;

            var allAtts = pi.GetCustomAttributes(false).ToArray();
            var cInfo = allAtts.OfType<ColumnInfoAttribute>().FirstOrDefault();
            if (cInfo != null)
                return cInfo;

            //recommend  System.Data.Linq.Mapping.ColumnAttribute. but It should be only Name property.
            var columnAtt = allAtts.SingleOrDefault(x => x.GetType().Name == "ColumnAttribute") as dynamic;
            if (columnAtt != null)
            {
                SetColumnInfoFromColumnAttribute(ret, columnAtt);
            }
            var dcc = allAtts.SingleOrDefault(x => x.GetType().FullName == "Dapper.Contrib.Extensions.ComputedAttribute");
            if (dcc != null)
            {
                ret.Ignore = true;
            }
            var key = allAtts.SingleOrDefault(x => x.GetType().FullName == "Dapper.Contrib.Extensions.KeyAttribute");
            if (dcc != null)
            {
                ret.IsPrimaryKey = true;
            }
            return ret;
        }
        private static void SetColumnInfoFromColumnAttribute(ColumnInfoAttribute info, dynamic att)
        {
            if (att == null) return;
            try
            {
                info.Name = att.Name;
            }
            catch { }

            try
            {
                info.Expression = att.Expression as string;
            }
            catch { }

            try
            {
                info.IsPrimaryKey = (bool)att.IsPrimaryKey;
            }
            catch { }

            try
            {
                info.IsVersion = (bool)att.IsVersion;
            }
            catch { }

            try
            {
                info.DbType = att.DbType as string;
            }
            catch { }
        }

        internal static ColumnInfoAttribute ToColumnInfo(this Expression expr)
        {
            var lambdaExp = (LambdaExpression)expr;
            var memExp = lambdaExp.Body as MemberExpression;
            if (memExp == null)
            {
                var convert = lambdaExp.Body as UnaryExpression;
                if (convert != null)
                {
                    memExp = convert.Operand as MemberExpression;
                }
            }
            return memExp.Member.CreateColumnInfo();
        }

        public static TResult Count<TResult>(this IDbConnection cnn, QueryImp query)
        {
            return cnn.ExecuteScalar<TResult>(string.Format("SELECT COUNT(1) FROM ({0}) T", query.SqlIgnoreOrderBy), query.Parameters);
        }

    }

    [AttributeUsage(AttributeTargets.Class)]
    public class TableAttribute : Attribute
    {
        public TableAttribute(string tableName)
        {
            Name = tableName;
        }
        public string Name { get; private set; }
    }
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnInfoAttribute : Attribute
    {
        public string Name { get; set; }
        public string Expression { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsVersion { get; set; }
        public bool Ignore { get; set; }
        public string DbType { get; set; }
    }
    internal class ColumnInfoCollection : List<ColumnInfoAttribute>
    {
        public string ToSelectClause()
        {
            var list = new List<string>();
            foreach (var each in this)
            {
                if (each.Ignore)
                    continue;

                var ret = each.Name;
                if (!string.IsNullOrWhiteSpace(each.Expression))
                {
                    ret = string.Format("({0}) AS {1}", each.Expression, each.Name);
                }
                list.Add(ret);
            }
            return string.Join(",", list);
        }
    }


    internal static class ILGeneratorUtil
    {
        private static readonly ConcurrentDictionary<Type, Type> TypeCache = new ConcurrentDictionary<Type, Type>();
        static ILGeneratorUtil()
        {
            var assemblyName = new AssemblyName(Guid.NewGuid().ToString());
            dynamicAssemblyBuilder = System.Threading.Thread.GetDomain().DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
        }
        private static readonly AssemblyBuilder dynamicAssemblyBuilder;

        public static bool IsInjected(Type type)
        {
            ValidColumnTypeMap(type);
            return TypeCache.ContainsKey(type);
        }

        public static Type InjectionInterfaceWithProperty(Type targetType)
        {
            Type buildType;
            if (TypeCache.TryGetValue(targetType, out buildType))
            {
                return buildType;
            }

            var moduleBuilder = dynamicAssemblyBuilder.DefineDynamicModule("ILGeneratorUtil." + targetType.Name);
            var typeBuilder = moduleBuilder.DefineType(targetType.Name + "_" + Guid.NewGuid(), TypeAttributes.Public | TypeAttributes.Class, targetType);

            var interfaceType = typeof(IContainerHolder);

            typeBuilder.AddInterfaceImplementation(interfaceType);
            foreach (var each in interfaceType.GetProperties())
            {
                BuildProperty(typeBuilder, each.Name, each.PropertyType);
            }
            buildType = typeBuilder.CreateType();
            TypeCache[targetType] = buildType;

            ValidColumnTypeMap(targetType);
            ValidColumnTypeMap(buildType);
            return buildType;
        }
        private static void BuildProperty(TypeBuilder typeBuilder, string name, Type type)
        {
            var field = typeBuilder.DefineField("_" + name.ToLower(), type, FieldAttributes.Private);
            var propertyBuilder = typeBuilder.DefineProperty(name, System.Reflection.PropertyAttributes.None, type, null);

            var getSetAttr = MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.SpecialName | MethodAttributes.Virtual;

            var getter = typeBuilder.DefineMethod("get_" + name, getSetAttr, type, Type.EmptyTypes);
            var getIL = getter.GetILGenerator();
            getIL.Emit(OpCodes.Ldarg_0);
            getIL.Emit(OpCodes.Ldfld, field);
            getIL.Emit(OpCodes.Ret);

            var setter = typeBuilder.DefineMethod("set_" + name, getSetAttr, null, new Type[] { type });
            var setIL = setter.GetILGenerator();
            setIL.Emit(OpCodes.Ldarg_0);
            setIL.Emit(OpCodes.Ldarg_1);
            setIL.Emit(OpCodes.Stfld, field);
            setIL.Emit(OpCodes.Ret);

            propertyBuilder.SetGetMethod(getter);
            propertyBuilder.SetSetMethod(setter);
        }

        private static void ValidColumnTypeMap(Type t)
        {
            if (!ColumnAttMapped.Contains(t))
            {
                Dapper.SqlMapper.SetTypeMap(t, new ColumnAttributeTypeMapper(t));
                ColumnAttMapped.Add(t);
            }
        }

        //http://stackoverflow.com/questions/8902674/manually-map-column-names-with-class-properties
        private static readonly ConcurrentBag<Type> ColumnAttMapped = new ConcurrentBag<Type>();
        private class ColumnAttributeTypeMapper : MultiTypeMapper
        {
            public ColumnAttributeTypeMapper(Type t)
                : base(new SqlMapper.ITypeMap[]
                {
                    new CustomPropertyTypeMap(t, FindPropertyInfo),                       
                    new DefaultTypeMap(t)
                })
            {
            }
            private static PropertyInfo FindPropertyInfo(Type t, string columnName)
            {
                try
                {
                    var properties = t.GetProperties(BindingFlags.NonPublic | BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static).ToArray();
                    foreach (var each in properties)
                    {
                        var atts = each.GetCustomAttributes(false).ToArray();
                        var columnAtt = atts.SingleOrDefault(x => x.GetType().Name == "ColumnAttribute") as dynamic;
                        if (columnAtt != null && string.Compare(columnAtt.Name, columnName, true) == 0) return each;
                        var columnInfo = atts.OfType<ColumnInfoAttribute>().FirstOrDefault();
                        if (columnInfo != null && string.Compare(columnInfo.Name, columnName, true) == 0) return each;
                    }
                }
                catch
                {
                }
                return null;
            }

        }
        private class MultiTypeMapper : SqlMapper.ITypeMap
        {
            private readonly IEnumerable<SqlMapper.ITypeMap> _mappers;
            public MultiTypeMapper(IEnumerable<SqlMapper.ITypeMap> mappers) { _mappers = mappers; }
            public ConstructorInfo FindConstructor(string[] names, Type[] types)
            {
                foreach (var mapper in _mappers)
                {
                    try
                    {
                        var result = mapper.FindConstructor(names, types);
                        if (result != null) return result;
                    }
                    catch (NotImplementedException) { }
                }
                return null;
            }
            public SqlMapper.IMemberMap GetConstructorParameter(ConstructorInfo constructor, string columnName)
            {
                foreach (var mapper in _mappers)
                {
                    try
                    {
                        var result = mapper.GetConstructorParameter(constructor, columnName);
                        if (result != null) return result;
                    }
                    catch (NotImplementedException) { }
                }
                return null;
            }
            public SqlMapper.IMemberMap GetMember(string columnName)
            {
                foreach (var mapper in _mappers)
                {
                    try
                    {
                        var result = mapper.GetMember(columnName);
                        if (result != null) return result;
                    }
                    catch (NotImplementedException) { }
                }
                return null;
            }
            public ConstructorInfo FindExplicitConstructor()
            {
                return _mappers.Select(mapper => mapper.FindExplicitConstructor()).FirstOrDefault(result => result != null);
            }
        }
    }


    public class Query<Root> : QueryImp
    {
        public Query()
        {
            RootType = typeof(Root);
            var atts = RootType.GetCustomAttributes(typeof(RelationAttribute), true).OfType<RelationAttribute>().ToArray();
            Relations.AddRange(atts);
        }

        public Criteria Eq<P>(Expression<Func<Root, P>> property, P obj)
        {
            return new IdCriteria(obj, property.ToColumnInfo().Name, ++CriteriaIndex);
        }
        public Criteria NotEq<P>(Expression<Func<Root, P>> property, P obj)
        {
            return new NotCriteria(new IdCriteria(obj, property.ToColumnInfo().Name, ++CriteriaIndex));
        }
        public Criteria Between<P>(Expression<Func<Root, P>> property, P start, P end)
        {
            return new BetweenCriteria(property.ToColumnInfo().Name, start, end, ++CriteriaIndex);
        }
        public Criteria In<P>(Expression<Func<Root, P>> property, params P[] args)
        {
            return new InCriteria(property.ToColumnInfo().Name, args.Cast<object>().ToList(), ++CriteriaIndex);
        }
        public Criteria Expression(string statemant, Dictionary<string, object> parameters = null)
        {
            return new ExpressionCriteria(statemant, parameters);
        }
        public Criteria Like<P>(Expression<Func<Root, P>> property, object obj, LikeCriteria.Match asterisk)
        {
            return new LikeCriteria(property.ToColumnInfo().Name, obj, asterisk, ++CriteriaIndex);
        }

        /// <summary>
        /// Property &gt; Value
        /// </summary>
        public Criteria GreaterThan<P>(Expression<Func<Root, P>> property, P obj)
        {
            return new ComparisonCriteria(property.ToColumnInfo().Name, obj,
                ComparisonCriteria.Comparison.GreaterThan, ComparisonCriteria.Eq.Ignore, ++CriteriaIndex);
        }
        /// <summary>
        /// Property ≧ Value
        /// </summary>
        public Criteria GreaterThanEq<P>(Expression<Func<Root, P>> property, P obj)
        {
            return new ComparisonCriteria(property.ToColumnInfo().Name, obj,
                ComparisonCriteria.Comparison.GreaterThan, ComparisonCriteria.Eq.Contains, ++CriteriaIndex);
        }
        /// <summary>
        /// Property &lt; Value
        /// </summary>
        public Criteria LessThan<P>(Expression<Func<Root, P>> property, P obj)
        {
            return new ComparisonCriteria(property.ToColumnInfo().Name, obj,
                ComparisonCriteria.Comparison.LessThan, ComparisonCriteria.Eq.Ignore, ++CriteriaIndex);
        }
        /// <summary>
        /// Property ≦ Value
        /// </summary>
        public Criteria LessThanEq<P>(Expression<Func<Root, P>> property, P obj)
        {
            return new ComparisonCriteria(property.ToColumnInfo().Name, obj,
                ComparisonCriteria.Comparison.LessThan, ComparisonCriteria.Eq.Contains, ++CriteriaIndex);
        }
        public Criteria IsNull<P>(Expression<Func<Root, P>> property)
        {
            return new ExpressionCriteria(string.Format(" {0} IS NULL ", property.ToColumnInfo().Name));
        }
        public Criteria IsNotNull<P>(Expression<Func<Root, P>> property)
        {
            return new ExpressionCriteria(string.Format(" {0} IS NOT NULL ", property.ToColumnInfo().Name));
        }

        public Query<Root> OrderBy<P>(Expression<Func<Root, P>> property)
        {
            Sorts.Add(string.Format("{0} ASC", property.ToColumnInfo().Name));
            return this;
        }
        public Query<Root> OrderByDesc<P>(Expression<Func<Root, P>> property)
        {
            Sorts.Add(string.Format("{0} DESC", property.ToColumnInfo().Name));
            return this;
        }
        public Query<Root> GroupBy<P>(Expression<Func<Root, P>> property)
        {
            Groups.Add(property.ToColumnInfo().Name);
            return this;
        }

        public Query<Root> SelectClauses<P>(Expression<Func<Root, P>> property)
        {
            var lambdaExp = (LambdaExpression)property;
            var memExp = lambdaExp.Body as MemberExpression;
            if (memExp == null)
            {
                var convert = lambdaExp.Body as UnaryExpression;
                if (convert != null)
                {
                    memExp = convert.Operand as MemberExpression;
                }
            }
            var column = memExp.Member.CreateColumnInfo();
            CustomSelectClauses.Add(column);

            return this;
        }
    }

    public abstract class QueryImp
    {
        internal int CriteriaIndex { get; set; }

        public QueryImp()
        {
            Relations = new List<RelationAttribute>();
            Sorts = new List<string>();
            Groups = new List<string>();
            CustomSelectClauses = new ColumnInfoCollection();
        }
        public List<RelationAttribute> Relations { get; private set; }
        public Type RootType { get; protected set; }
        public string Sql
        {
            get
            {
                return string.Format("SELECT {0} {1} FROM {2} {3} {4} {5} {6}",
                    SelectTopClause, SelectClause, TableClause, WhereClause, GroupByClause, HavingClause, OrderByClause);
            }
        }
        public string SqlIgnoreOrderBy
        {
            get
            {
                return string.Format("SELECT {0} {1} FROM {2} {3} {4} {5}",
                    SelectTopClause, SelectClause, TableClause, WhereClause, GroupByClause, HavingClause);
            }
        }
        protected string SelectClause
        {
            get
            {
                var selectClause = string.Empty;
                if (CustomSelectClauses.Any())
                {
                    selectClause = CustomSelectClauses.ToSelectClause();
                }
                else
                {
                    selectClause = RootType.GetSelectClause().ToSelectClause();
                }
                return selectClause;
            }
        }
        internal ColumnInfoCollection CustomSelectClauses { get; set; }


        public string SelectTopClause { get; set; }
        protected string TableClause { get { return RootType.GetTableName(); } }
        protected string WhereClause
        {
            get
            {
                var where = string.Empty;
                if (Filter != null)
                {
                    where = string.Format(" WHERE {0}", Filter.BuildStatement());
                }
                return where;
            }
        }
        protected string OrderByClause
        {
            get
            {
                var orderBy = string.Empty;
                if (Sorts.Any())
                {
                    orderBy = string.Format(" ORDER BY {0}", string.Join(",", Sorts));
                }
                return orderBy;
            }
        }
        protected string GroupByClause
        {
            get
            {
                var groupBy = string.Empty;
                if (Groups.Any())
                {
                    groupBy = string.Format(" GROUP BY {0}", string.Join(",", Groups));
                }
                return groupBy;
            }
        }
        protected string HavingClause
        {
            get
            {
                var having = string.Empty;
                if (Having != null)
                {
                    having = string.Format(" HAVING {0}", Having.BuildStatement());
                }
                return having;
            }
        }


        internal object Parameters
        {
            get
            {
                var dic = new Dictionary<string, object>();
                if (Filter != null)
                {
                    foreach (var each in Filter.BuildParameters())
                    {
                        dic[each.Key] = each.Value;
                    }
                }
                if (Having != null)
                {
                    foreach (var each in Having.BuildParameters())
                    {
                        dic[each.Key] = each.Value;
                    }
                }
                return dic.Keys.Any() ? dic : null;
            }
        }
        public Criteria Filter { get; set; }
        protected List<string> Sorts { get; private set; }
        protected List<string> Groups { get; private set; }
        public Criteria Having { get; set; }
        public QueryImp Join<Parent, Child>(Expression<Func<Parent, object>> parentProperty = null, Expression<Func<Child, object>> childProperty = null)
        {
            return Join<Parent, Child>(parentProperty.ToColumnInfo().Name, childProperty.ToColumnInfo().Name);
        }
        public QueryImp Join<Parent, Child>(string parentPropertyName, string childPropertyName)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), parentPropertyName, childPropertyName));
            return this;
        }


        public QueryImp Join<Parent, Child>(string key, Expression<Func<Parent, object>> parentProperty = null, Expression<Func<Child, object>> childProperty = null)
        {
            return Join<Parent, Child>(key, parentProperty.ToColumnInfo().Name, childProperty.ToColumnInfo().Name);
        }
        public QueryImp Join<Parent, Child>(string key, string parentPropertyName, string childPropertyName)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), key, new[] { parentPropertyName }, new[] { childPropertyName }));
            return this;
        }


        public QueryImp Join<Parent, Child>(Expression<Func<Parent, object>>[] parentProperties = null, Expression<Func<Child, object>>[] childProperties = null)
        {
            return Join<Parent, Child>(
                parentProperties.Select(x => x.ToColumnInfo().Name).ToArray(),
                childProperties.Select(x => x.ToColumnInfo().Name).ToArray());
        }
        public QueryImp Join<Parent, Child>(string[] parentPropertyNames, string[] childPropertyNames)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), parentPropertyNames, childPropertyNames));
            return this;
        }

        public QueryImp Join<Parent, Child>(string key, Expression<Func<Parent, object>>[] parentProperties = null, Expression<Func<Child, object>>[] childProperties = null)
        {
            return Join<Parent, Child>(
                key,
                parentProperties.Select(x => x.ToColumnInfo().Name).ToArray(),
                childProperties.Select(x => x.ToColumnInfo().Name).ToArray());
        }
        public QueryImp Join<Parent, Child>(string key, string[] parentPropertyNames, string[] childPropertyNames)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), key, parentPropertyNames, childPropertyNames));
            return this;
        }





    }


    internal class DapperDataAdapter
    {
        public int SplitCount
        {
            get { return _splitCount; }
            set { _splitCount = value; }
        }
        private int _splitCount = 100;
        public int QueryOptimizerLevel
        {
            get { return _queryOptimizerLevel; }
            set { _queryOptimizerLevel = value; }
        }
        private int _queryOptimizerLevel = 100;

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

        public List<object> Fill(IDbConnection cnn, CommandDefinition command, RelationAttribute[] atts)
        {
            var result = new List<object>();
            var tableType = relationAttribute.ChildType;
            var tableName = relationAttribute.ChildTableName;
            var clause = tableType.GetSelectClause().ToSelectClause();
            var splitCriteria = SplitCriteria();


            if (QueryOptimizerLevel < splitCriteria.Count)
            {
                //nest query pattern
                var stackCriteria = new Stack<NestCriteria>();

                Type type = relationAttribute.ParentType;
                var criteria = new NestCriteria(relationAttribute);

                stackCriteria.Push(criteria);
                while (TryFindNestQuery(atts, ref type, ref criteria))
                {
                    stackCriteria.Push(criteria);
                }

                var sql = string.Empty;
                var count = stackCriteria.Count;
                for (int i = 0; i < count; i++)
                {
                    var c = stackCriteria.Pop();
                    if (i == 0)
                    {
                        c.View = command.CommandText;
                    }
                    else
                    {
                        c.View = sql;
                    }
                    sql = string.Format(" SELECT {0} FROM {1} WHERE {2}",
                            c.Att.ChildType.GetSelectClause().ToSelectClause(),
                            c.Att.ChildTableName,
                            c.BuildStatement());
                }
                var rows = cnn.Query(tableType, sql, command.Parameters, command.Transaction, command.Buffered, command.CommandTimeout, command.CommandType);
                result.AddRange(rows);
            }
            else
            {
                //id query pattern
                foreach (var each in splitCriteria)
                {
                    var statement = each.BuildStatement();
                    var param = each.BuildParameters();
                    var sql = string.Format("SELECT {0} FROM {1} WHERE {2} ", clause, tableName, statement);
                    var rows = cnn.Query(tableType, sql, param, command.Transaction, command.Buffered, command.CommandTimeout, command.CommandType);
                    result.AddRange(rows);
                }
            }
            return result;
        }

        private bool TryFindNestQuery(RelationAttribute[] atts, ref Type type, ref NestCriteria criteria)
        {
            foreach (var each in atts.Where(x => x.Loaded))
            {
                if (each.ChildType == type)
                {
                    criteria = new NestCriteria(each);
                    type = each.ParentType;
                    return true;
                }
            }
            return false;
        }

        private List<Criteria> SplitCriteria()
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
            ParentTableName = ParentType.GetTableName();
            ChildTableName = ChildType.GetTableName();

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
        public void EnsureDynamicType()
        {
            if (!ParentType.GetInterfaces().Any(x => x == typeof(IContainerHolder)))
            {
                ParentType = ILGeneratorUtil.InjectionInterfaceWithProperty(ParentType);
            }
            if (!ChildType.GetInterfaces().Any(x => x == typeof(IContainerHolder)))
            {
                ChildType = ILGeneratorUtil.InjectionInterfaceWithProperty(ChildType);
            }
        }

        [NonSerialized]
        internal DapperDataAdapter DataAdapter;
        static string CreateDefaultKey(Type parentType, Type childType)
        {
            return string.Format("{0}.{1}", parentType.Name, childType.Name);
        }
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
                {
                    var newT = ILGeneratorUtil.InjectionInterfaceWithProperty(typeof(T));
                    att = Atts.FirstOrDefault(x => x.ChildType == newT);
                }
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
        public static Criteria operator !(Criteria c)
        {
            return new NotCriteria(c);
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
    public class NotCriteria : Criteria
    {
        Criteria c;
        public NotCriteria(Criteria c)
        {
            this.c = c;
        }
        public override string BuildStatement()
        {
            return string.Format(" NOT ({0})", c.BuildStatement());
        }
        public override Dictionary<string, object> BuildParameters()
        {
            return c.BuildParameters();
        }
    }

    public class BetweenCriteria : Criteria
    {
        public string Name { get; private set; }
        public object Start { get; private set; }
        public object End { get; private set; }
        public int Index { get; private set; }
        public BetweenCriteria(string name, object start, object end, int index = 0)
        {
            this.Name = name;
            this.Start = start;
            this.End = end;
            this.Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} BETWEEN @startP{1} AND @endP{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@startP{0}", Index)] = Start;
            dic[string.Format("@endP{0}", Index)] = End;
            return dic;
        }
    }

    public class ExpressionCriteria : Criteria
    {
        public string Statemant { get; private set; }
        public Dictionary<string, object> Parameters { get; private set; }
        public ExpressionCriteria(string statemant, Dictionary<string, object> parameters = null)
        {
            Statemant = statemant;
            Parameters = parameters;
        }
        public override string BuildStatement()
        {
            return Statemant;
        }
        public override Dictionary<string, object> BuildParameters()
        {
            return Parameters ?? new Dictionary<string, object>();
        }
    }

    public class LikeCriteria : Criteria
    {
        public enum Match
        {
            Start,
            End,
            Match
        }


        public string Name { get; private set; }
        public object Value { get; private set; }
        public Match Mat { get; private set; }
        public int Index { get; private set; }
        public LikeCriteria(string name, object value, Match match, int index = 0)
        {
            this.Name = name;
            this.Value = value;
            this.Mat = match;
            this.Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} LIKE @p{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@p{0}", Index)] =
                string.Format("{0}{1}{2}", Mat == Match.End ? string.Empty : "%", Value, Mat == Match.Start ? string.Empty : "%");
            return dic;
        }

        private static string Escape(string s)
        {
            return s.Replace("%", "[%]").Replace("[", "[[]").Replace("]", "[]]");
        }

    }

    public class ComparisonCriteria : Criteria
    {
        public enum Eq
        {
            Contains,
            Ignore
        }
        public enum Comparison
        {
            /// <summary>
            /// Property &gt; Value
            /// </summary>
            GreaterThan,
            /// <summary>
            /// Property &lt; Value
            /// </summary>
            LessThan
        }

        public string Name { get; private set; }
        public object Value { get; private set; }
        public Comparison Comp { get; private set; }
        public Eq EqStatus { get; private set; }
        public int Index { get; private set; }
        public ComparisonCriteria(string name, object value, Comparison comparison, Eq eq, int index = 0)
        {
            Name = name;
            Value = value;
            Comp = comparison;
            EqStatus = eq;
            Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} {1}{2} @p{3}", Name, Comp == Comparison.GreaterThan ? ">" : "<", EqStatus == Eq.Contains ? "=" : string.Empty, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@p{0}", Index)] = Value;
            return dic;
        }
    }


    public class InCriteria : Criteria
    {
        public string Name { get; private set; }
        public List<object> InList { get; private set; }
        public int Index { get; private set; }
        public InCriteria(string name, List<object> inList, int index = 0)
        {
            Name = name;
            InList = inList;
            Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} IN @p{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@p{0}", Index)] = InList;
            return dic;
        }
    }

    internal class NestCriteria : Criteria
    {
        public string View { get; set; }
        public RelationAttribute Att { get; private set; }
        public NestCriteria(RelationAttribute att)
        {
            Att = att;
        }

        public override Dictionary<string, object> BuildParameters()
        {
            return new Dictionary<string, object>();
        }

        public override string BuildStatement()
        {
            var sql = string.Empty;

            var list = new List<string>();
            for (int i = 0; i < Att.ParentPropertyNames.Length; i++)
            {
                var parentProperty = Att.ParentPropertyNames[i];
                var childProperty = Att.ChildPropertyNames[i];

                list.Add(string.Format(" {0}.{1} = {2}.{3}", Att.ParentTableName, parentProperty, Att.ChildTableName, childProperty));
            }

            sql = string.Format(" EXISTS(SELECT 1 FROM {0} {1} WHERE {2})",
                string.IsNullOrWhiteSpace(View) ? string.Empty : string.Format("({0})", View),
                Att.ParentTableName,
                string.Join(" AND ", list));

            return sql;
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
        public static Criteria Not(this Criteria criteria)
        {
            return new NotCriteria(criteria);
        }
    }

    #endregion

}
