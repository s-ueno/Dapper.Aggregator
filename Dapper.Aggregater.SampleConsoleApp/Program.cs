using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using Dapper.Aggregater.SampleConsoleApp.Model;
using System.Diagnostics;
using System.Collections;
using System.Data;

namespace Dapper.Aggregater.SampleConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            TransAction(AggregateWithPoco);
            TransAction(AggregateWithImplementInterface);
        }

        private static void AggregateWithPoco(IDbConnection sqlMapper)
        {
            var query = new Query<EventTable>();
            query.Join<EventTable, EventDetailsTable>("EventTableID", "EventTableID");
            query.Join<EventDetailsTable, CodeTable>(parent => parent.CodeTableID, child => child.CodeTableCD);

            query.Filter = query.Eq(x => x.EventTableID, 0) |
                           query.NotEq(x => x.EventTableID, 1) &
                           query.Between(x => x.EventTime, DateTime.Now.AddYears(-10), DateTime.Now.AddYears(10)) &
                           query.In(x => x.EventTableID, 0, 10000) &
                           !query.Like(x => x.EventTitle, "AAAAA", LikeCriteria.Match.Start) |
                           query.LessThan(x => x.EventTableID, 100) &
                           query.IsNotNull(x => x.EventTableID) &
                           query.Expression(" EXISTS(SELECT 1 FROM EventDetailsTable WHERE EventTable.EventTableID = EventDetailsTable.EventTableID)");

            //debug statement
            Trace.TraceInformation(query.Filter.BuildStatement());
            Trace.TraceInformation(query.Sql);

            var rows = sqlMapper.QueryWith(query);
            foreach (var row in rows)
            {
                foreach (var each in (row as IContainerHolder).Container.GetChildren<EventDetailsTable>())
                {
                    foreach (var item in (each as IContainerHolder).Container.GetChildren<CodeTable>())
                    {
                    }
                }
            }
        }

        private static void AggregateWithImplementInterface(IDbConnection sqlMapper)
        {
            var rows = sqlMapper.QueryWith<DefinedAggregate>(@"select * from EventTable");
            foreach (var row in rows)
            {
                foreach (var each in row.Details)
                {
                    foreach (var item in (each as IContainerHolder).Container.GetChildren<CodeTable>())
                    {
                    }
                }
            }
        }


        public static void TransAction(Action<IDbConnection> action)
        {
            using (var helper = DbConnectionRepository.CreateDbHelper())
            {
                helper.Open();
                try
                {
                    helper.BeginTransaction();

                    action(helper.DbConnection);

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
    }
}
