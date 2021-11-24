using System.Collections.Generic;

namespace Dapper.Aggregator
{
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

}
