using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace Dapper.Aggregator
{
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

        public Criteria Exists(Type childType, string[] sourceProperties, string[] targetProperties, Criteria childCriteria = null)
        {
            var parentTableName = this.TableClause;
            var childTableName = childType.GetTableName();

            var parentTableAliasName = EscapeAliasFormat(parentTableName);
            var childTableTableAliasName = EscapeAliasFormat(childTableName);

            if (!sourceProperties.Any() || !targetProperties.Any())
                throw new ArgumentException("Columns is required.");

            if (sourceProperties.Length != targetProperties.Length)
                throw new ArgumentException("Columns is mismatch.");

            var parentColumns = typeof(Root).GetSelectClause().ToDictionary(x => x.PropertyInfoName);
            var childColumns = childType.GetSelectClause().ToDictionary(x => x.PropertyInfoName);

            var list = new List<string>();
            for (int i = 0; i < sourceProperties.Length; i++)
            {
                list.Add(string.Format("({0}.{1} = {2}.{3})", parentTableAliasName, parentColumns[sourceProperties[i]].Name, childTableTableAliasName, childColumns[targetProperties[i]].Name));
            }
            if (childCriteria != null)
            {
                list.Add(childCriteria.BuildStatement());
            }

            var sql = string.Format("EXISTS(SELECT 1 FROM {0} {1} WHERE {2})", childTableName, childTableTableAliasName, string.Join(" AND ", list));
            return new ExpressionCriteria(sql, childCriteria != null ? childCriteria.BuildParameters() : null);
        }

    }
}
