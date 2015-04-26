# Dapper.Aggregater

I'm trying to add a entity-relationship to Dapper


you can get a defined entity-relationship using "QueryWith" method.
I improve Criteria Pattern a little more and consider that I can easily make Sql.


Dynamic aggregate-pattern
--------
all poco
```csharp
    public class EventTable
    {
        public int EventTableID { get; set; }
        public DateTime EventTime { get; set; }
        public string EventTitle { get; set; }
        public byte[] Lockversion { get; set; }
    }
    public class EventDetailsTable
    {
        public int EventTableID { get; set; }
        public int EventDetailsTableID { get; set; }
        public int CodeTableID { get; set; }
        public byte[] Lockversion { get; set; }
    }
    public class CodeTable
    {
        public int CodeTableCD { get; set; }
        public string CodeTableName { get; set; }
        public byte[] Lockversion { get; set; }
    }    
```

Dynamic aggregate-pattern Example
--------
and simple poco Criteria Pattern 


```csharp
var query = new Query<EventTable>();
query.Join<EventTable, EventDetailsTable>("EventTableID", "EventTableID");
query.Join<EventDetailsTable, CodeTable>("CodeTableID", "CodeTableCD");

query.Filter = query.Eq(x => x.EventTableID, 0) |
               query.NotEq(x => x.EventTableID, 1) &
               query.Between(x => x.EventTime, DateTime.Now.AddYears(-10), DateTime.Now.AddYears(10)) &
               query.In(x => x.EventTableID, 0, 10000) &
               !query.Like(x => x.EventTitle, "AAAAA", LikeCriteria.Match.Start) |
               query.LessThan(x => x.EventTableID, 100) &
               query.IsNotNull(x => x.EventTableID);

var rows = sqlMapper.QueryWith(query);
foreach (var row in rows)
{
    // Using 'TypeBuilder', inject interface
    foreach (var each in (row as IContainerHolder).Container.GetChildren<EventDetailsTable>())
    {
        foreach (var item in (each as IContainerHolder).Container.GetChildren<CodeTable>())
        {
        }
    }
}
```



Defined aggregate-pattern
--------
[EventTable]
  -[EventDetailsTable]
    -[CodeTable]

When you create EntityClass with an automatic generation tool, attach "IContainerHolder" together.
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
                    _details = (this as IContainerHolder).Container.GetChildren<EventDetailsTable>().ToArray();
                }
                return _details;
            }
        }
        private EventDetailsTable[] _details = null;

    }
```

Defined aggregate-pattern Example
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



I am interested in using Dapper.
I want to try to challenge myself to various things.
