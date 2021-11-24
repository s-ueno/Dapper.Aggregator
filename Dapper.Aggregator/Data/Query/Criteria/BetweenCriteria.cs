using System.Collections.Generic;

namespace Dapper.Aggregator
{
    public class BetweenCriteria : Criteria
    {
        public string Name { get; private set; }
        public object Start { get; private set; }
        public object End { get; private set; }
        public int Index { get; private set; }
        public BetweenCriteria(string name, object start, object end, int index = 0)
        {
            this.Name = name;
            this.Start = start;
            this.End = end;
            this.Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} BETWEEN @startP{1} AND @endP{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@startP{0}", Index)] = Start;
            dic[string.Format("@endP{0}", Index)] = End;
            return dic;
        }
    }

}
