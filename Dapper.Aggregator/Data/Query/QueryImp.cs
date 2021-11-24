using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{

    public abstract class QueryImp
    {
        internal protected Type RootType { get; protected set; }
        internal protected int CriteriaIndex { get; set; }

        public int? StartRecord { get; set; }
        public int? MaxRecord { get; set; }

        public List<RelationAttribute> Relations { get; private set; }
        public List<string> Sorts { get; private set; }
        public List<string> Groups { get; private set; }
        public QueryImp()
        {
            Relations = new List<RelationAttribute>();
            Sorts = new List<string>();
            Groups = new List<string>();
            CustomSelectClauses = new ColumnInfoCollection();
        }
        public virtual string Sql
        {
            get
            {
                return string.Format("{0} {1} ", SqlIgnoreOrderBy, OrderByClause);
            }
        }
        public virtual string SqlIgnoreOrderBy
        {
            get
            {
                var sql = string.Format("SELECT {0} {1} FROM {2} {3} {4} {5} {6}",
                                SelectTopClause, SelectClause, TableClause, EscapeAliasFormat(TableClause), WhereClause, GroupByClause, HavingClause);

                if (StartRecord.HasValue || MaxRecord.HasValue)
                {
                    if (!SelectClauseCollection.Any(x => x.IsPrimaryKey) && !Sorts.Any())
                        throw new InvalidOperationException("StartRecord or MaxRecord need to set the primary key or Sort key");

                    sql = string.Format("SELECT ROW_NUMBER() OVER (ORDER BY {0}) AS buff_rowNum, T.* FROM ({1}) T ",
                                        Sorts.Any() ? string.Join(",", Sorts) : string.Join(",", SelectClauseCollection.Where(x => x.IsPrimaryKey).Select(x => x.Name)), sql);

                    var where = new List<string>();
                    if (StartRecord.HasValue)
                    {
                        where.Add(string.Format("{0} <= buff_rowNum", StartRecord.Value));
                    }
                    if (MaxRecord.HasValue)
                    {
                        where.Add(string.Format("buff_rowNum <= {0}", MaxRecord.Value));
                    }
                    sql = string.Format("SELECT {0} FROM ({1}) T WHERE {2}", SelectClause, sql, string.Join(" AND ", where));
                }
                return sql;
            }
        }

        internal protected virtual string SelectClause
        {
            get
            {
                return SelectClauseCollection.ToSelectClause();
            }
        }
        internal ColumnInfoCollection CustomSelectClauses { get; set; }

        internal protected virtual ColumnInfoCollection SelectClauseCollection
        {
            get
            {
                return CustomSelectClauses.Any() ? CustomSelectClauses : RootType.GetSelectClause();
            }
        }


        public string SelectTopClause { get; set; }

        internal protected virtual string TableClause { get { return RootType.GetTableName(); } }

        internal protected virtual string EscapeAliasFormat(string s)
        {
            s = s.Replace("\"", "\"\"");
            return $"\"{s}\"";
        }

        internal protected virtual string WhereClause
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
        internal protected virtual string OrderByClause
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
        internal protected virtual string GroupByClause
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
        internal protected virtual string HavingClause
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

        internal protected virtual Dictionary<string, object> Parameters
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
        public Criteria Having { get; set; }
        public QueryImp Join<Parent, Child>(Expression<Func<Parent, object>> parentProperty = null, Expression<Func<Child, object>> childProperty = null)
        {
            return Join<Parent, Child>(parentProperty.ToColumnInfo().PropertyInfoName, childProperty.ToColumnInfo().PropertyInfoName);
        }
        public QueryImp Join<Parent, Child>(string parentPropertyName, string childPropertyName)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), parentPropertyName, childPropertyName));
            return this;
        }


        public QueryImp Join<Parent, Child>(string key, Expression<Func<Parent, object>> parentProperty = null, Expression<Func<Child, object>> childProperty = null)
        {
            return Join<Parent, Child>(key, parentProperty.ToColumnInfo().PropertyInfoName, childProperty.ToColumnInfo().PropertyInfoName);
        }
        public QueryImp Join<Parent, Child>(string key, string parentPropertyName, string childPropertyName)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), key, new[] { parentPropertyName }, new[] { childPropertyName }));
            return this;
        }


        public QueryImp Join<Parent, Child>(Expression<Func<Parent, object>>[] parentProperties = null, Expression<Func<Child, object>>[] childProperties = null)
        {
            return Join<Parent, Child>(
                parentProperties.Select(x => x.ToColumnInfo().PropertyInfoName).ToArray(),
                childProperties.Select(x => x.ToColumnInfo().PropertyInfoName).ToArray());
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
                parentProperties.Select(x => x.ToColumnInfo().PropertyInfoName).ToArray(),
                childProperties.Select(x => x.ToColumnInfo().PropertyInfoName).ToArray());
        }
        public QueryImp Join<Parent, Child>(string key, string[] parentPropertyNames, string[] childPropertyNames)
        {
            Relations.Add(new RelationAttribute(typeof(Parent), typeof(Child), key, parentPropertyNames, childPropertyNames));
            return this;
        }

        public void Ensure(int splitCount = 100, int optimizerLevel = 10, bool injectionDynamicType = true)
        {
            foreach (var each in Relations)
            {
                if (each.ParentType == null)
                    each.ParentType = RootType;

                each.Ensure(this);
                if (injectionDynamicType)
                    each.EnsureDynamicType();
                each.DataAdapter.SplitCount = splitCount;
                each.DataAdapter.QueryOptimizerLevel = optimizerLevel;
            }
        }

    }
}
