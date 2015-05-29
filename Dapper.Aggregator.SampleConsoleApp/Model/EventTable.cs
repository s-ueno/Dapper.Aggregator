using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregator.SampleConsoleApp.Model
{
    public class EventTable
    {
        public int EventTableID { get; set; }
        public DateTime EventTime { get; set; }
        public string EventTitle { get; set; }
        public byte[] Lockversion { get; set; }
    }
}
