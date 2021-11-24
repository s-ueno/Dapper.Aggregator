using System.Collections.Generic;

namespace Dapper.Aggregator
{
    public class LikeCriteria : Criteria
    {
        public enum Match
        {
            Start,
            End,
            Match
        }


        public string Name { get; private set; }
        public object Value { get; private set; }
        public Match Mat { get; private set; }
        public int Index { get; private set; }
        public LikeCriteria(string name, object value, Match match, int index = 0)
        {
            this.Name = name;
            this.Value = value;
            this.Mat = match;
            this.Index = index;
        }
        public override string BuildStatement()
        {
            return string.Format(" {0} LIKE @p{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic[string.Format("@p{0}", Index)] =
                string.Format("{0}{1}{2}", Mat == Match.End ? string.Empty : "%", Value, Mat == Match.Start ? string.Empty : "%");
            return dic;
        }

        private static string Escape(string s)
        {
            return s.Replace("%", "[%]").Replace("[", "[[]").Replace("]", "[]]");
        }

    }

}
