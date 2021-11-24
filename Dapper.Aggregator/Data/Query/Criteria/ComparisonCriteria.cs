using System.Collections.Generic;

namespace Dapper.Aggregator
{
    public class ComparisonCriteria : Criteria
    {
        public enum Eq
        {
            Contains,
            Ignore
        }
        public enum Comparison
        {
            /// <summary>
            /// Property &gt; Value
            /// </summary>
            GreaterThan,
            /// <summary>
            /// Property &lt; Value
            /// </summary>
            LessThan
        }

        public string Name { get; private set; }
        public object Value { get; private set; }
        public Comparison Comp { get; private set; }
        public Eq EqStatus { get; private set; }
        public int Index { get; private set; }
        public ComparisonCriteria(string name, object value, Comparison comparison, Eq eq, int index = 0)
        {
            Name = name;
            Value = value;
            Comp = comparison;
            EqStatus = eq;
            Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} {1}{2} @p{3}", Name, Comp == Comparison.GreaterThan ? ">" : "<", EqStatus == Eq.Contains ? "=" : string.Empty, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@p{0}", Index)] = Value;
            return dic;
        }
    }

}
