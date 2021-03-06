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
simple poco Pattern 

```csharp
var query = new Query<EventTable>();

//[EventTable]-[EventDetailsTable]
query.Join<EventTable, EventDetailsTable>("EventTableID", "EventTableID");

//[EventDetailsTable]-[CodeTable]
query.Join<EventDetailsTable, CodeTable>(parent => parent.CodeTableID, child => child.CodeTableCD);

var rows = sqlMapper.QueryWith(query);
foreach (var row in rows)
{
    // If the class does not implement interface(IContainerHolder), I embed interface dynamically using TypeBuilder.
    foreach (var each in (row as IContainerHolder).Container.GetChildren<EventDetailsTable>())
    {
        foreach (var item in (each as IContainerHolder).Container.GetChildren<CodeTable>())
        {
        }
    }
}
```
Other features 

Criteria Pattern 
```csharp

var query = new Query<EventTable>();
query.Join<EventTable, EventDetailsTable>("EventTableID", "EventTableID");
query.Join<EventDetailsTable, CodeTable>(parent => parent.CodeTableID, child => child.CodeTableCD);

//build criteria
query.Filter = query.Eq(x => x.EventTableID, 0) |
               query.NotEq(x => x.EventTableID, 1) &
               query.Between(x => x.EventTime, DateTime.Now.AddYears(-10), DateTime.Now.AddYears(10)) &
               query.In(x => x.EventTableID, 0, 10000) &
               !query.Like(x => x.EventTitle, "AAAAA", LikeCriteria.Match.Start) |
               query.LessThan(x => x.EventTableID, 100) &
               query.IsNotNull(x => x.EventTableID) &
               query.Expression(" EXISTS(SELECT 1 FROM EventDetailsTable WHERE EventTable.EventTableID = EventDetailsTable.EventTableID)");

query.GroupBy(x => x.EventTableID)
     .GroupBy(x => x.EventTime)
     .GroupBy(x => x.EventTitle)
     .GroupBy(x => x.Lockversion);

query.Having = query.Eq(x => x.EventTableID, 3);

query.OrderBy(x => x.EventTableID)
     .OrderByDesc(x => x.EventTitle);
     
var rows = sqlMapper.QueryWith(query);
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

var query = new Query<EventAggregate>();
//It is not necessary to use the Join method
var rows = sqlMapper.QueryWith(query);
foreach (var each in rows)
{
    foreach (var detail in each.Details)
    {
        foreach (var item in (detail as IContainerHolder).Container.GetChildren<CodeTable>())
        {
            
        }
    }
}

```



I am interested in using Dapper.
I want to try to challenge myself to various things.
