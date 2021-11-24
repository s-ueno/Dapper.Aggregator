using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    [Serializable]
    [AttributeUsage(AttributeTargets.Class)]
    public class IndexAttribute : Attribute
    {
        public string Name { get; set; }
        public string[] Columns { get; set; }
        public bool IsUnique { get; set; } = false;
        public IndexAttribute(string name, string[] columns)
        {
            Name = name;
            Columns = columns;  
        }
    }
}
