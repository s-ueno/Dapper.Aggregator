using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{

    [Serializable]
    public class ColumnInfoCollection : List<ColumnAttribute>
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
                else
                {
                    ret = string.Format("{0} AS {1}", each.Name, each.Name);
                }
                list.Add(ret);
            }
            return string.Join(",", list);
        }
    }
}
