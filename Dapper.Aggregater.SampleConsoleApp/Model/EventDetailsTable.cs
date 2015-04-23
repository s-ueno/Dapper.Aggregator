using Dapper.Contrib.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregater.SampleConsoleApp.Model
{
    [Table("EventDetailsTable")]
    public class EventDetailsTable : IContainerHolder
    {
        public int EventTableID { get; set; }
        public int EventDetailsTableID { get; set; }
        public int CodeTableID { get; set; }
        public byte[] Lockversion { get; set; }

        DataContainer IContainerHolder.Container { get; set; }
    }
}
