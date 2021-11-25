using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
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
                var cpi = att.childPropertyAccessors[i];
                var pi = att.parentPropertyAccessors[i];
                dataParameter.Add(new DapperDataParameter(cpi.Att.Name, pi));
            }
        }

        List<Criteria> childCriteriaList = new List<Criteria>();
        int idIndex = 0;
        public void AssignDataParameter(object value)
        {
            var list = new List<Criteria>();
            foreach (var each in dataParameter)
            {
                list.Add(each.CreateIdCriteria(value, ++idIndex));
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

        async public Task<List<object>> FillAsync(IDbConnection cnn, CommandDefinition command, RelationAttribute[] atts)
        {
            using var activity = DapperExtensions._activitySource.StartActivity("FillAsync", ActivityKind.Internal);

            var result = new List<object>();
            var tableType = relationAttribute.ChildType;
            var newTableType = ILGeneratorUtil.IsInjected(tableType) ? ILGeneratorUtil.InjectionInterfaceWithProperty(tableType) : tableType;

            var tableName = relationAttribute.ChildTableName;
            var tableAliasName = relationAttribute.ChildAliasTableName;
            var clause = newTableType.GetSelectClause().ToSelectClause();
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
                    sql = string.Format(" SELECT {0} FROM {1} {2} WHERE {3}",
                            c.Att.ChildType.GetSelectClause().ToSelectClause(),
                            c.Att.ChildTableName,
                            c.Att.ChildAliasTableName,
                            c.BuildStatement());
                }

                activity?.AddTag("Sql", sql);

                var rows = await cnn.QueryAsync(newTableType, sql, command.Parameters, command.Transaction, command.CommandTimeout, command.CommandType);
                result.AddRange(rows);
            }
            else
            {
                //id query pattern
                foreach (var each in splitCriteria)
                {
                    var statement = each.BuildStatement();
                    var param = each.BuildParameters();
                    var sql = string.Format("SELECT {0} FROM {1} {2} WHERE {3} ", clause, tableName, tableAliasName, statement);

                    activity?.AddTag("Sql", sql);

                    var rows = await cnn.QueryAsync(newTableType, sql, param, command.Transaction, command.CommandTimeout, command.CommandType);
                    result.AddRange(rows);
                }
            }

            if (activity?.IsAllDataRequested == true)
            {
                activity?.AddTag("Fill Count", result.Count);
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
}
