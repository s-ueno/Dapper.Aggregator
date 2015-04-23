using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregater.SampleConsoleApp.Model
{
    [Table("EventTable")]
    public class EventTable : IContainerHolder
    {
        public int EventTableID { get; set; }
        public DateTime EventTime { get; set; }
        public string EventTitle { get; set; }
        public byte[] Lockversion { get; set; }

        DataContainer IContainerHolder.Container { get; set; }
    }
}
