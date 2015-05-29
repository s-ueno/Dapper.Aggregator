using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using Dapper.Aggregator;
namespace Dapper.Aggregator.SampleConsoleApp.Model
{
    public class CodeTable
    {
        public int CodeTableCD { get; set; }
        public string CodeTableName { get; set; }
        public byte[] Lockversion { get; set; }
    }
}
