# Dapper.Aggregater
tried to add a DataRelation to Dapper


you can get a defined entity relation using "QueryWith" method.
I improve Criteria Pattern a little more and consider that I can easily make Sql.


[EventTable]
  -[EventDetailsTable]
    -[CodeTable]

```csharp
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
```

