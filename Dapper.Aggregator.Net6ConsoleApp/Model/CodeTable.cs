using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator.Net6ConsoleApp.Model
{
    [Table("code_table")]
    public class CodeTable
    {
        [Column(Name = "cd", DDLType = "char(10)", IsPrimaryKey = true)]
        public string CD { get; set; }

        [Column(Name = "name", DDLType = "varchar(50)")]
        public string Name { get; set; }

        [Column(Name = "lockversion", DDLType = "int", IsVersion = true)]
        public int Lockversion { get; set; }
    }
}
