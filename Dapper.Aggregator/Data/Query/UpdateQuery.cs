using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;

namespace Dapper.Aggregator
{
    public class UpdateQuery<T> : Query<T>
    {
        public UpdateQuery<T> Set<P>(Expression<Func<T, P>> property, P obj)
        {
            setClauses.Add(new SetClausesHolder(property.ToColumnInfo().Name, obj, ++CriteriaIndex));
            return this;
        }
        private List<SetClausesHolder> setClauses = new List<SetClausesHolder>();
        internal protected virtual string UpdateClauses
        {
            get
            {
                var ret = string.Format("UPDATE {0} SET {1} {2}", TableClause, string.Join(",", setClauses.Select(x => x.Clauses)), WhereClause);
                return ret;
            }
        }
        protected internal override Dictionary<string, object> Parameters
        {
            get
            {
                var dic = base.Parameters ?? new Dictionary<string, object>();
                foreach (var each in setClauses)
                {
                    dic[each.Placeholder] = each.Value;
                }
                return dic;
            }
        }
    }
}
