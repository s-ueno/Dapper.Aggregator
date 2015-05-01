using Dapper.Aggregater.SampleConsoleApp.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregater.SampleConsoleApp
{

    [Relation(typeof(EventDetailsTable_Holder), typeof(CodeTable), "CodeTableID", "CodeTableCD")]
    [Relation(typeof(EventDetailsTable_Holder), "EventTableID", "EventTableID")]
    public class DefinedAggregate : EventTable_Holder
    {
        [Column(Ignore=true)]
        public EventDetailsTable[] Details
        {
            get
            {
                if (_details == null)
                {
                    _details = (this as IContainerHolder).Container.GetChildren<EventDetailsTable_Holder>().ToArray();
                }
                return _details;
            }
        }
        private EventDetailsTable[] _details = null;
    }

  
    //When you create EntityClass with an automatic generation tool, attach "IContainerHolder" together.

    [Table("EventTable")]
    public class EventTable_Holder : EventTable, IContainerHolder
    {
        DataContainer IContainerHolder.Container { get; set; }
    }
    [Table("EventDetailsTable")]
    public class EventDetailsTable_Holder : EventDetailsTable, IContainerHolder
    {
        DataContainer IContainerHolder.Container { get; set; }
    }
}
