using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{

    internal class SetClausesHolder
    {
        public SetClausesHolder(string setClauses, object value, int index = 0)
        {
            SetClauses = setClauses;
            Value = value;
            Index = index;
        }
        public string SetClauses { get; private set; }
        public object Value { get; private set; }
        public int Index { get; private set; }
        internal string Placeholder
        {
            get
            {
                return $"@{SetClauses}_{Index}";
                //return string.Format("@{0}{1}", "p", Index);
            }
        }
        internal string Clauses { get { return string.Format("{0} = {1}", SetClauses, Placeholder); } }
    }
}
