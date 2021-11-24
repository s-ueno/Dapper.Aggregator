using System.Collections.Generic;

namespace Dapper.Aggregator
{
    public class ExpressionCriteria : Criteria
    {
        public string Statemant { get; private set; }
        public Dictionary<string, object> Parameters { get; private set; }
        public ExpressionCriteria(string statemant, Dictionary<string, object> parameters = null)
        {
            Statemant = statemant;
            Parameters = parameters;
        }
        public override string BuildStatement()
        {
            return Statemant;
        }
        public override Dictionary<string, object> BuildParameters()
        {
            return Parameters ?? new Dictionary<string, object>();
        }
    }

}
