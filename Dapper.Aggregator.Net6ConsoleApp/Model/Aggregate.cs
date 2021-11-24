using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator.Net6ConsoleApp.Model
{
    [Relation(typeof(EventDetailsTable), nameof(CodeTable), nameof(EventDetailsTable.CD), nameof(CodeTable.CD))]
    [Relation(typeof(EventDetailsTable), nameof(EventTable.ID), nameof(EventDetailsTable.EventID))]
    public class DefinedAggregate : EventTable { }
}
