using System;
using System.Collections.Generic;

namespace Dapper.Aggregator
{
    public class IdCriteria : Criteria, IEquatable<IdCriteria>
    {
        public IdCriteria(object value, string name, int index = 0)
        {
            Value = value;
            Name = name;
            Index = index;
        }
        public object Value { get; private set; }
        public string Name { get; private set; }
        public int Index { get; private set; }

        public override string BuildStatement()
        {
            return string.Format("{0} = @p{1}", Name, Index);
        }
        public override Dictionary<string, object> BuildParameters()
        {
            var dic = new Dictionary<string, object>();
            dic.Add(string.Format("@p{0}", Index), Value);
            return dic;
        }
        public override int GetHashCode()
        {
            var hash = 0;
            if (Value != null)
            {
                hash = Value.GetHashCode();
            }
            hash = hash ^ Name.GetHashCode();
            return hash;
        }
        public override bool Equals(object obj)
        {
            return (this as IEquatable<IdCriteria>).Equals(obj as IdCriteria);
        }
        bool IEquatable<IdCriteria>.Equals(IdCriteria other)
        {
            if (other == null) return false;
            if (object.ReferenceEquals(this, other)) return true;

            if (Value == null)
            {
                return other.Value == null && Name == other.Name;
            }

            return Value.Equals(other.Value) && Name == other.Name;
        }
    }

}
