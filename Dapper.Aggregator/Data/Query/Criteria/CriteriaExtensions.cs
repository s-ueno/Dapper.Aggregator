using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    public static class CriteriaExtensions
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
}
