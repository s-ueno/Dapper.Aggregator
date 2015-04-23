using Dapper.Aggregater.SampleConsoleApp.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dapper.Aggregater.SampleConsoleApp
{

    [Relation(typeof(EventDetailsTable), typeof(CodeTable), "CodeTableID", "CodeTableCD")]
    [Relation(typeof(EventDetailsTable), "EventTableID", "EventTableID")]
    public class EventAggregate : EventTable
    {
        public EventDetailsTable[] Details
        {
            get
            {
                if (_details == null)
                {
                    _details = (this as IContainerHolder).Container.GetChildren<EventDetailsTable>();
                }
                return _details;
            }
        }
        private EventDetailsTable[] _details = null;

    }
}
