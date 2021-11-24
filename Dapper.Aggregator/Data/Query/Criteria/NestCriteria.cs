using System.Collections.Generic;

namespace Dapper.Aggregator
{
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
            for (int i = 0; i < Att.parentPropertyAccessors.Count; i++)
            {
                var parentProperty = Att.parentPropertyAccessors[i];
                var childProperty = Att.childPropertyAccessors[i];

                list.Add(string.Format(" {0}.{1} = {2}.{3}", Att.ParentAliasTableName, parentProperty.Att.Name, Att.ChildAliasTableName, childProperty.Att.Name));
            }

            sql = string.Format(" EXISTS(SELECT 1 FROM {0} {1} WHERE {2})",
                string.IsNullOrWhiteSpace(View) ? string.Empty : string.Format("({0})", View),
                Att.ParentAliasTableName,
                string.Join(" AND ", list));

            return sql;
        }
    }

}
