using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using Dapper.Aggregater;
using Dapper.Contrib.Extensions;
namespace Dapper.Aggregater.SampleConsoleApp.Model
{

    [Table("CodeTable")]
    public class CodeTable : IContainerHolder
    {
        public int CodeTableCD { get; set; }
        public string CodeTableName { get; set; }
        public byte[] Lockversion { get; set; }

        DataContainer IContainerHolder.Container { get; set; }
    }
}
