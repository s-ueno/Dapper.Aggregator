using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dapper;
using Dapper.Aggregater.SampleConsoleApp.Model;
using System.Diagnostics;

namespace Dapper.Aggregater.SampleConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            TransAction(SampleLogic);
        }
        private static void SampleLogic(DbConnectionHelper helper)
        {
            var sqlMapper = helper.DbConnection;
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
