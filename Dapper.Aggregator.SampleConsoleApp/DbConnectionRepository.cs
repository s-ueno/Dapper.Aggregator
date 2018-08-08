using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dapper.Aggregator.SampleConsoleApp
{
    public static class DbConnectionRepository
    {
        public const string DefaultContext = "Default";
        public static ConnectionStringSettings CreateConnectionString(string contextName = DefaultContext)
        {
            return ConfigurationManager.ConnectionStrings[contextName];
        }
        public static DbProviderFactory CreateDbProviderFactory()
        {
            var con = CreateConnectionString();
            return CreateDbProviderFactory(con.ProviderName);
        }
        public static DbProviderFactory CreateDbProviderFactory(string providerName)
        {
            return DbProviderFactories.GetFactory(providerName);
        }
        public static DbConnectionHelper CreateDbHelper(string contextName = DefaultContext)
        {
            return new DbConnectionHelper(contextName);
        }
    }

    public class DbConnectionHelper : IDisposable
    {
        public DbConnection DbConnection { get; private set; }
        protected internal string ContextName { get; internal set; }
        internal DbTransaction Transaction { get; private set; }
        public DbConnectionHelper(string context)
        {
            ContextName = context;
        }
        public void Open()
        {
            var info = DbConnectionRepository.CreateConnectionString(ContextName);
            var fac = DbConnectionRepository.CreateDbProviderFactory(info.ProviderName);

            DbConnection = fac.CreateConnection();
            DbConnection.ConnectionString = info.ConnectionString;

            DbConnection.Open();
        }
        public void BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted)
        {
            Transaction = DbConnection.BeginTransaction(isolationLevel);
        }
        public void Commit()
        {
            Transaction.Commit();
        }
        public void Rollback()
        {
            Transaction.Rollback();
        }
        public void Close()
        {
            if (closed) return;

            if (Transaction != null)
            {
                Transaction.Dispose();
                Transaction = null;
            }
            DbConnection.Close();
            closed = true;
        }
        bool closed = false;
        public DbCommand CreateCommand()
        {
            return DbConnection.CreateCommand();
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                Close();
            }
            disposed = true;
        }
        bool disposed = false;
    }
}
