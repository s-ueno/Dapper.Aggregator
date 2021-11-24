using System.Collections.Generic;

namespace Dapper.Aggregator
{
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

}
