using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregater.SampleConsoleApp.Model
{
    public class EventDetailsTable
    {
        public int EventTableID { get; set; }
        public int EventDetailsTableID { get; set; }
        public int CodeTableID { get; set; }
        public byte[] Lockversion { get; set; }
    }
}
