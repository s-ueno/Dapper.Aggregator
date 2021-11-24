using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator.Net6ConsoleApp
{
    internal class DbConnectionFactory
    {
        public DbConnectionFactory(IServiceProvider provider)
        {
            Provider = provider;
        }
        public IServiceProvider Provider { get; private set; }

        public const string DefaultContext = "ConnectionString";
        public const string DefaultProvider = "ProviderName";

        public DbConnectionHelper CreateHelper(string contextName = DefaultContext, string providerName = DefaultProvider)
        {
            var config = Provider.GetService<IConfiguration>();

            var con = config.GetValue<String>($"values:{contextName}");
            var provider = config.GetValue<String>($"values:{providerName}");

            var svc = new DbConnectionHelper(con, provider);
            return svc;
        }

    }

    public class DbConnectionHelper : IDisposable
    {
        protected internal string ConnectionString { get; internal set; } = "";
        protected internal string ProviderName { get; internal set; } = "";

        public DbConnectionHelper(string connectionString, string providerName)
        {
            ConnectionString = connectionString;
            ProviderName = providerName;

            var factory = DbProviderFactories.GetFactory(ProviderName);
            if (factory == null)
                throw new ArgumentException(nameof(providerName));

            DbConnection = factory.CreateConnection();
            DbConnection.ConnectionString = ConnectionString;
        }


        public DbConnection DbConnection { get; private set; }

        internal DbTransaction? Transaction { get; private set; }

        public void Open()
        {
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
