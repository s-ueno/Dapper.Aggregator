using Dapper;
using Dapper.Aggregator;
using Dapper.Aggregator.Net6ConsoleApp;
using Dapper.Aggregator.Net6ConsoleApp.Model;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Data;
using System.Reflection;
using System.Linq;

var startup = new Startup();
IServiceProvider provider = startup.Ensure();




var codeList = Enumerable.Range(0, 1000000 /* million */).Select(i =>
{
    return new CodeTable
    {
        CD = i.ToString().PadLeft(10, '0'),
        Name = $"Name - {i}",
        Lockversion = 0
    };
});

await TransAction(async (sqlMapper, transaction) => {

    var rows = await sqlMapper.QueryAsync("select * from code_table limit 100 ", transaction);
    var json = System.Text.Json.JsonSerializer.Serialize(rows);
    foreach (var each in rows)
    {
        Console.WriteLine(each["cd"]);
    }

});

await TransAction(async (sqlMapper, transaction) =>
{
    var row = await sqlMapper.FindIdAsync<CodeTable>(keys: 0.ToString().PadLeft(10, '0'));
    row.Name = "AAA";

    Console.WriteLine($"Lockversion:{row.Lockversion}");
    await sqlMapper.UpdateEntityAsync(row);
    Console.WriteLine($"Lockversion:{row.Lockversion}");

    await sqlMapper.DeleteEntityAsync(row);

    await sqlMapper.TableLock<CodeTable>();

});


// Drop Table 
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.DropIfExistsTableAsync<CodeTable>();
    await sqlMapper.DropIfExistsTableAsync<EventTable>();
    await sqlMapper.DropIfExistsTableAsync<EventDetailsTable>();
});

// Create Table or Index
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.CreateIfNotExistsTableAsync<CodeTable>();
    await sqlMapper.CreateIfNotExistsTableAsync<EventTable>();
    await sqlMapper.CreateIfNotExistsTableAsync<EventDetailsTable>();
});

// Bulk INSERT
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.BulkInsertAsync(codeList, transaction, maximumParameterizedCount: 10000);

    await sqlMapper.BulkInsertAsync(Enumerable.Range(0, 1000000 /* million */).Select(i =>
    {
        return new EventTable
        {
            ID = i,
            EventTime = DateTime.Now,
            Title = $"Title - {i}",
            Lockversion = 0
        };
    }), maximumParameterizedCount: 10000);

    await sqlMapper.BulkInsertAsync(Enumerable.Range(0, 1000000 /* million */).Select(i =>
    {
        return new EventDetailsTable
        {
            EventID = i,
            DetailNo = i,
            CD = i.ToString().PadLeft(10, '0'),
            Lockversion = 0
        };
    }), transaction, maximumParameterizedCount: 10000);
});

// Bulk UPDATE
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.BulkUpdateAsync(codeList, transaction, maximumParameterizedCount: 10000, commandTimeout: 0);
});
// Bulk DELETE
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.BulkDeleteAsync(codeList, transaction, maximumParameterizedCount: 10000, commandTimeout: 0);
});
// Bulk INSERT
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.BulkInsertAsync(codeList, transaction, maximumParameterizedCount: 10000, commandTimeout: 0);
});

await TransAction(async (sqlMapper, transaction) =>
{
    var query = new Query<EventTable>();
    query.Join<EventTable, EventDetailsTable>(nameof(EventTable.ID), nameof(EventDetailsTable.EventID));
    query.Join<EventDetailsTable, CodeTable>(parent => parent.CD, child => child.CD);

    query.Filter = query.Eq(x => x.ID, 0) |
                   query.NotEq(x => x.ID, 1) &
                   query.Between(x => x.EventTime, DateTime.Now.AddYears(-10), DateTime.Now.AddYears(10)) &
                   query.In(x => x.ID, 1000, 2000) &
                   !query.Like(x => x.Title, "AAAAA", LikeCriteria.Match.Start) |
                   query.LessThan(x => x.ID, 100) &
                   query.IsNotNull(x => x.ID) &
                   query.Exists(typeof(EventDetailsTable), new[] { nameof(EventTable.ID) }, new[] { nameof(EventDetailsTable.EventID) });
    //query.Expression(" EXISTS(SELECT 1 FROM EventDetailsTable WHERE EventTable.EventTableID = EventDetailsTable.EventTableID)");

    query.GroupBy(x => x.ID)
         .GroupBy(x => x.EventTime)
         .GroupBy(x => x.Title)
         .GroupBy(x => x.Lockversion);

    // query.Having = query.Eq(x => x.ID, 3);

    query.OrderBy(x => x.ID)
         .OrderByDesc(x => x.Title);

    var rows = await sqlMapper.FindAsync(query, transaction);
    foreach (var row in rows)
    {
        foreach (var each in (row as IContainerHolder).Container.GetChildren<EventDetailsTable>())
        {
            foreach (var item in (each as IContainerHolder).Container.GetChildren<CodeTable>())
            {
                Console.WriteLine($"{row.ID} {each.DetailNo} {item.Name}");
            }
        }
    }
});

// Truncate Table 
await TransAction(async (sqlMapper, transaction) =>
{
    await sqlMapper.TruncateAsync<CodeTable>();
    await sqlMapper.TruncateAsync<EventTable>();
    await sqlMapper.TruncateAsync<EventDetailsTable>();
});






async Task TransAction(Func<IDbConnection, IDbTransaction, Task> action)
{
    var factory = provider.GetService<DbConnectionFactory>();
    using (var helper = factory.CreateHelper())
    {
        helper.Open();
        try
        {
            helper.BeginTransaction();

            await action(helper.DbConnection, helper.Transaction);

            helper.Commit();
        }
        catch (Exception)
        {
            helper.Rollback();

            throw;
        }
        finally
        {
            helper.Close();
        }
    }
}
