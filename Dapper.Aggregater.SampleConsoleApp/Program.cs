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

            query.GroupBy(x => x.EventTableID)
                 .GroupBy(x => x.EventTime)
                 .GroupBy(x => x.EventTitle)
                 .GroupBy(x => x.Lockversion);

            query.Having = query.Eq(x => x.EventTableID, 3);

            query.OrderBy(x => x.EventTableID)
                 .OrderByDesc(x => x.EventTitle);

            //debug statement
            Trace.TraceInformation(query.Filter.BuildStatement());
            Trace.TraceInformation(query.Sql);


            //nest query pattern(performance)
            //var rows = sqlMapper.QueryWith(query, splitLength: 1, queryOptimizerLevel: 1);


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
