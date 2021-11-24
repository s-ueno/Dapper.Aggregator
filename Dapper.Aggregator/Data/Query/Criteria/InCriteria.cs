using System.Collections.Generic;

namespace Dapper.Aggregator
{

    public class InCriteria : Criteria
    {
        public string Name { get; private set; }
        public List<object> InList { get; private set; }
        public int Index { get; private set; }
        public InCriteria(string name, List<object> inList, int index = 0)
        {
            Name = name;
            InList = inList;
            Index = index;
        }
        public override string BuildStatement()
        {
            var subP = new List<string>();
            for (int i = 0; i < InList.Count; i++)
            {
                subP.Add(string.Format("@p{0}{1}", Index, i));
            }
            return string.Format(" {0} IN ({1})", Name, string.Join(",", subP));
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            for (int i = 0; i < InList.Count; i++)
            {
                dic[string.Format("@p{0}{1}", Index, i)] = InList[i];
            }
            return dic;
        }
    }

}
