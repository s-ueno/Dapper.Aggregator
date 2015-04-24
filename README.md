# Dapper.Aggregater
tried to add a entity-relationship to Dapper


you can get a defined entity-relationship using "QueryWith" method.
I improve Criteria Pattern a little more and consider that I can easily make Sql.


Definition
--------
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

Example
--------
```csharp
var rows = sqlMapper.QueryWith<EventAggregate>(@"select * from EventTable");

foreach (var row in rows)
{
    Trace.TraceInformation(string.Format("EventTable.EventTableID => {0} EventTitle => {1}", row.EventTableID, row.EventTitle));
    foreach (var each in row.Details)
    {
        Trace.TraceInformation(string.Format("EventDetailsTable.EventTableID => {0} EventDetailsTableID => {1}", each.EventTableID, each.EventDetailsTableID));
        foreach (var item in (each as IContainerHolder).Container.GetChildren<CodeTable>())
        {
            Trace.TraceInformation(string.Format("CodeTable.CodeTableCD => {0} CodeTableName => {1}", item.CodeTableCD, item.CodeTableName));
        }
    }
}
```


