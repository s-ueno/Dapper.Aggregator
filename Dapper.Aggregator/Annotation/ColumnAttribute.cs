using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    [Serializable]
    [AttributeUsage(AttributeTargets.Property)]
    public class ColumnAttribute : Attribute
    {
        public string Name { get; set; }
        public string DDLType { get; set; }
        public string PropertyInfoName { get; set; }
        public string Expression { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsVersion { get; set; }
        public bool Ignore { get; set; }
        public string DbType { get; set; }
        public bool CanBeNull { get; set; }
        public string Description { get; set; }
    }
}
