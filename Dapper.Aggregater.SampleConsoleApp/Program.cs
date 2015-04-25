using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using Dapper.Aggregater.SampleConsoleApp.Model;
using System.Diagnostics;
using System.Collections;

namespace Dapper.Aggregater.SampleConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            TransAction(AggregateWithPoco);
            TransAction(AggregateWithImplementInterface);
        }

        private static void AggregateWithPoco(DbConnectionHelper helper)
        {
            var sqlMapper = helper.DbConnection;

            var query = new Query<EventTable>();
            query.Join<EventTable, EventDetailsTable>("EventTableID", "EventTableID");
            query.Join<EventDetailsTable, CodeTable>("CodeTableID", "CodeTableCD");

            //query.Sql = "select * from EventTable where EventTableID = @p1";
            //query.Parameters = new { p1 = 1 };

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

        private static void AggregateWithImplementInterface(DbConnectionHelper helper)
        {
            var sqlMapper = helper.DbConnection;
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


        public static void TransAction(Action<DbConnectionHelper> action)
        {
            using (var helper = DbConnectionRepository.CreateDbHelper())
            {
                helper.Open();
                try
                {
                    helper.BeginTransaction();

                    action(helper);

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
