using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dapper.Aggregator
{
    public class OperatorCriteria : Criteria, IEquatable<OperatorCriteria>
    {
        public Criteria[] Args { get; private set; }
        public string Operator { get; private set; }
        public OperatorCriteria(string @operator, params Criteria[] args)
        {
            Operator = @operator;
            Args = args.Where(x => x != null).ToArray();
        }
        public override string BuildStatement()
        {
            if (Args == null || !Args.Any()) return null;
            if (Args.Length == 1)
                return Args.First().BuildStatement();

            var list = new List<string>();
            foreach (var each in Args)
            {
                var statement = each.BuildStatement();
                if (!string.IsNullOrWhiteSpace(statement))
                    list.Add(string.Format("({0})", statement));
            }
            return string.Join(" " + Operator + " ", list);
        }

        public override int GetHashCode()
        {
            var hash = 0;
            hash = Operator.GetHashCode();
            foreach (var each in Args)
            {
                hash = hash ^ each.GetHashCode();
            }
            return hash;
        }
        public override bool Equals(object obj)
        {
            return (this as IEquatable<OperatorCriteria>).Equals(obj as OperatorCriteria);
        }
        bool IEquatable<OperatorCriteria>.Equals(OperatorCriteria other)
        {
            if (other == null) return false;
            if (object.ReferenceEquals(this, other)) return true;
            return Operator == other.Operator && Args.SequenceEqual(other.Args);
        }

        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            foreach (var each in Args)
            {
                var parameters = each.BuildParameters();
                foreach (var kp in parameters)
                {
                    dic.Add(kp.Key, kp.Value);
                }
            }
            return dic;
        }
    }

}
