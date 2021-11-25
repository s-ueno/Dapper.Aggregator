using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{   
    [Serializable]
    [AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public class ColumnAttribute : Attribute
    {
        private readonly int _order;
        public ColumnAttribute([CallerLineNumber] int order = 0) 
        {
            _order = order;
        }
        public int Order { get { return _order; } }

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
