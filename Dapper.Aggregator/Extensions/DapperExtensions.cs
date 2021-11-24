using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

//poco pattern
// If the class does not implement interface(IContainerHolder), I embed interface dynamically using TypeBuilder.

namespace Dapper.Aggregator
{
    public static class DapperExtensions
    {
        internal static readonly ActivitySource _activitySource = new ActivitySource("Dapper.Aggregator");

        #region FindAsync

        async public static Task<IEnumerable<T>> FindAsync<T>(this IDbConnection cnn, Query<T> query,
            IDbTransaction transaction = null, int? commandTimeout = null, int splitLength = 100, int queryOptimizerLevel = 10)
        {
            using var activity = _activitySource.StartActivity("FindAsync", ActivityKind.Internal);
            activity?.AddTag("Sql", query.Sql);

            query.Ensure(splitLength, queryOptimizerLevel);

            var oldType = query.RootType;
            var newParentType = ILGeneratorUtil.IsInjected(oldType) ? ILGeneratorUtil.InjectionInterfaceWithProperty(oldType) : oldType;

            var rows = await cnn.QueryAsync<T>(query.Sql, query.Parameters, transaction, commandTimeout);
            if (rows != null && rows.Any())
            {
                var command = new CommandDefinition(query.SqlIgnoreOrderBy, query.Parameters, transaction, commandTimeout);
                await LoadAsync(cnn, command, newParentType, query.Relations.ToArray(), rows);
            }
            return rows;
        }

        async private static Task LoadAsync(IDbConnection cnn, CommandDefinition command, Type t, RelationAttribute[] atts, System.Collections.IEnumerable roots)
        {
            var rootAtts = atts.Where(x => !x.Loaded && x.ParentType == t).ToArray();
            if (!rootAtts.Any()) return;

            var rootDataStore = new DataStore();
            var enumerator = roots.GetEnumerator();
            var hasValue = enumerator.MoveNext();
            while (hasValue)
            {
                var value = enumerator.Current;
                var holder = value as IContainerHolder;
                if (holder != null)
                {
                    holder.Container = new DataContainer(value, rootAtts, rootDataStore);
                }
                foreach (var att in rootAtts)
                {
                    att.DataAdapter.AssignDataParameter(value);
                }
                hasValue = enumerator.MoveNext();
            }

            foreach (var att in rootAtts)
            {
                var list = await att.DataAdapter.FillAsync(cnn, command, atts);
                att.Loaded = true;
                rootDataStore.Add(att.Key, list);
                if (list.Count != 0)
                {
                    await LoadAsync(cnn, command, att.ChildType, atts, list);
                }
            }
        }

        #endregion

        #region ScalarAsync

        async public static Task<T> ScalarAsync<T>(this IDbConnection cnn, QueryImp query, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("ScalarWith", ActivityKind.Internal);
            activity?.AddTag("Sql", query.Sql);

            return await cnn.ExecuteScalarAsync<T>(query.Sql, query.Parameters, transaction, commandTimeout, CommandType.Text);
        }

        #endregion

        #region DeleteQueryAsync

        async public static Task<int> DeleteQueryAsync(this IDbConnection cnn, QueryImp query,
            IDbTransaction transaction = null, int? commandTimeout = null, bool isRootOnly = true)
        {
            using var activity = _activitySource.StartActivity("DeleteQuery", ActivityKind.Internal);

            if (!isRootOnly)
            {
                query.Ensure(injectionDynamicType: false);
                var rootView = string.Format("SELECT {0} FROM {1} {2}", query.SelectClause, query.TableClause, query.WhereClause);
                var atts = query.Relations.ToList();
                var list = new List<DeleteQueryObject>();
                foreach (var each in query.Relations)
                {
                    if (each.ParentType == query.RootType)
                    {
                        atts.Remove(each);
                        RecursiveDeleteQuery(atts, list, new DeleteQueryObject(each, rootView, 0));
                    }
                }
                foreach (var each in list.OrderByDescending(x => x.NestLevel))
                {
                    cnn.Execute(each.DeleteClause, query.Parameters, transaction, commandTimeout);
                }
            }
            var rootDeleteSql = string.Format("DELETE FROM {0} {1}", query.TableClause, query.WhereClause);
            return await cnn.ExecuteAsync(rootDeleteSql, query.Parameters, transaction, commandTimeout);
        }

        private static void RecursiveDeleteQuery(List<RelationAttribute> atts, List<DeleteQueryObject> items, DeleteQueryObject rd)
        {
            items.Add(rd);
            var arr = atts.ToArray();
            foreach (var each in arr)
            {
                if (each.ParentType == rd.Criteria.Att.ChildType)
                {
                    atts.Remove(each);
                    RecursiveDeleteQuery(atts, items, new DeleteQueryObject(each, rd.View, rd.NestLevel + 1));
                }
            }
        }

        #endregion

        #region UpdateQueryAsync

        async public static Task<int> UpdateQueryAsync<T>(this IDbConnection cnn, UpdateQuery<T> query,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("UpdateQuery", ActivityKind.Internal);
            activity?.AddTag("Sql", query.UpdateClauses);

            return await cnn.ExecuteAsync(query.UpdateClauses, query.Parameters, transaction, commandTimeout);
        }

        #endregion

        #region CreateIfNotExistsTableAsync

        async public static Task<int> CreateIfNotExistsTableAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var type = typeof(T);
            var table = type.GetTableName();
            var indexes = type.GetIndexes();
            var columns = type.GetSelectClause();
            if (!columns.All(x =>
                !String.IsNullOrWhiteSpace(x.Name) &&
                !String.IsNullOrWhiteSpace(x.DDLType)
            ))
            {
                throw new InvalidOperationException("Add Name and DDLType to the ColumnAttribute.");
            }
            var primary = columns.Where(x => x.IsPrimaryKey).Select(x => x.Name).ToArray();
            var ddlColumns = columns.Select(x => toDDLColumn(x));
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TABLE IF NOT EXISTS {table}(");
            sb.AppendLine($"{String.Join(",", ddlColumns)}");
            if (primary.Any())
            {
                sb.AppendLine($", PRIMARY KEY({string.Join(",", primary)})");
            }
            sb.AppendLine($");");

            foreach (var each in indexes)
            {
                var unique = each.IsUnique ? "UNIQUE" : "";
                sb.AppendLine($"CREATE {unique} INDEX IF NOT EXISTS {each.Name} ON {table}  (");
                sb.AppendLine($"{string.Join(",", each.Columns)}");
                sb.AppendLine($");");
            }

            using var activity = _activitySource.StartActivity("CreateIfNotExistsTableAsync", ActivityKind.Internal);
            activity?.AddTag("Sql", sb.ToString());

            return await cnn.ExecuteAsync(sb.ToString(), null, transaction, commandTimeout);
        }

        private static string toDDLColumn(ColumnAttribute column)
        {
            if (column.Ignore) return "";

            var list = new List<string>();
            list.Add(column.Name);
            list.Add(column.DDLType);
            if (!column.CanBeNull)
            {
                list.Add("NOT NULL");
            }
            return String.Join(" ", list);
        }

        #endregion

        #region CountAsync

        async public static Task<int> CountAsync<T>(this IDbConnection cnn, Query<T> query)
        {
            var sql = string.Format("SELECT COUNT(1) FROM ({0}) T", query.SqlIgnoreOrderBy);

            using var activity = _activitySource.StartActivity("Count", ActivityKind.Internal);
            activity?.AddTag("Sql", sql);

            return await cnn.ExecuteScalarAsync<int>(sql, query.Parameters);
        }

        async public static Task<TResult> CountAsync<TResult>(this IDbConnection cnn, QueryImp query)
        {
            var sql = string.Format("SELECT COUNT(1) FROM ({0}) T", query.SqlIgnoreOrderBy);

            using var activity = _activitySource.StartActivity("Count", ActivityKind.Internal);
            activity?.AddTag("Sql", sql);

            return await cnn.ExecuteScalarAsync<TResult>(sql, query.Parameters);
        }

        #endregion

        #region InsertEntityAsync / BulkInsertAsync

        async public static Task<int> InsertEntityAsync<T>(this IDbConnection cnn, T entity,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            return await BulkInsertAsync(cnn, new T[] { entity }, transaction, 1, commandTimeout);
        }

        async public static Task<int> BulkInsertAsync<T>(this IDbConnection cnn, IEnumerable<T> entities,
            IDbTransaction transaction = null, int maximumParameterizedCount = 1000, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("BulkInsertAsync", ActivityKind.Internal);

            if (entities == null || !entities.Any())
                throw new ArgumentNullException("entities");

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            var versionPolicy = AttributeUtil.Find<VersionPolicyAttribute>(type);

            var result = 0;
            var chunkSize = Math.Max(maximumParameterizedCount / columns.Length, 1);
            foreach (var group in entities.Chunk(chunkSize))
            {
                var items = group.ToArray();
                var dic = new Dictionary<string, object>();
                var parameterizedValues = new List<string>();
                for (int i = 0; i < items.Length; i++)
                {
                    var entity = items[i];
                    foreach (var validator in validators)
                    {
                        var validateError = validator.GetError(entity);
                        if (validateError != null)
                        {
                            throw validateError;
                        }
                    }


                    var list = new List<string>();
                    foreach (var column in columns)
                    {
                        if (column.Ignore) continue;

                        var value = accessors[column.PropertyInfoName].GetValue(entity);
                        if (column.IsVersion)
                        {
                            foreach (var policy in versionPolicy)
                            {
                                value = policy.Generate(value);
                            }
                        }

                        var clauses = new SetClausesHolder(column.Name, value, i);
                        dic[clauses.Placeholder] = clauses.Value;
                        list.Add(clauses.Placeholder);
                    }
                    parameterizedValues.Add($"({string.Join(",", list)})");
                }

                var sql = string.Format("INSERT INTO {0} ({1}) VALUES {2} ;", table, string.Join(",", columns.Where(x => !x.Ignore).Select(x => x.Name)), string.Join(",", parameterizedValues));
                result += await cnn.ExecuteAsync(sql, dic, transaction, commandTimeout);
            }


            return result;
        }

        #endregion

        #region UpdateEntityAsync / BulkUpdateAsync

        async public static Task UpdateEntityAsync<T>(this IDbConnection cnn, T entity,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            await BulkUpdateAsync(cnn, new T[] { entity }, transaction, 1, commandTimeout);
        }

        async public static Task BulkUpdateAsync<T>(this IDbConnection cnn, IEnumerable<T> entities,
            IDbTransaction transaction = null, int maximumParameterizedCount = 1000, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("BulkUpdateAsync", ActivityKind.Internal);

            if (entities == null || !entities.Any())
                throw new ArgumentNullException("entity");

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            if (!columns.Any(x => x.IsPrimaryKey))
                throw new System.Data.MissingPrimaryKeyException("should have a primary key.");

            var setColumns = columns.Where(x => !x.IsPrimaryKey && !x.Ignore).ToArray();
            var whereColumns = columns.Where(x => (x.IsPrimaryKey || x.IsVersion) && !x.Ignore).ToArray();

            var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            var versionPolicy = AttributeUtil.Find<VersionPolicyAttribute>(type);

            var chunkSize = Math.Max(maximumParameterizedCount / (setColumns.Length + whereColumns.Length), 1);
            foreach (var group in entities.Chunk(chunkSize))
            {
                var items = group.ToArray();
                var dic = new Dictionary<string, object>();
                var sqlList = new List<string>();
                var clausesIndex = 0;
                for (int i = 0; i < items.Length; i++)
                {
                    var entity = items[i];
                    foreach (var validator in validators)
                    {
                        var validateError = validator.GetError(entity);
                        if (validateError != null)
                        {
                            throw validateError;
                        }
                    }

                    var setList = new List<SetClausesHolder>();
                    clausesIndex += 1;
                    foreach (var column in setColumns)
                    {
                        var value = accessors[column.PropertyInfoName].GetValue(entity);
                        if (column.IsVersion)
                        {
                            foreach (var policy in versionPolicy)
                            {
                                value = policy.Generate(value);
                            }
                        }

                        var setClauses = new SetClausesHolder(column.Name, value, i);
                        dic[setClauses.Placeholder] = setClauses.Value;
                        setList.Add(setClauses);
                    }

                    var whereList = new List<SetClausesHolder>();
                    clausesIndex += 1;
                    foreach (var column in whereColumns)
                    {
                        var value = accessors[column.PropertyInfoName].GetValue(entity);
                        var whereClauses = new SetClausesHolder(column.Name, value, clausesIndex);
                        dic[whereClauses.Placeholder] = whereClauses.Value;
                        whereList.Add(whereClauses);
                    }

                    var sql = string.Format("UPDATE {0} SET {1} WHERE {2} ;", table,
                                string.Join(",", setList.Select(x => x.Clauses)),
                                string.Join(" AND ", whereList.Select(x => x.Clauses)));
                    sqlList.Add(sql);
                }

                var count = await cnn.ExecuteAsync(String.Join(Environment.NewLine, sqlList), dic, transaction, commandTimeout);
                if (count != items.Length)
                    throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");
            }
        }

        #endregion

        #region DeleteEntityAsync / BulkDeleteAsync

        async public static Task DeleteEntityAsync<T>(this IDbConnection cnn, T entity,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            await BulkDeleteAsync(cnn, new T[] { entity }, transaction, 1, commandTimeout);
        }

        async public static Task BulkDeleteAsync<T>(this IDbConnection cnn, IEnumerable<T> entities,
           IDbTransaction transaction = null, int maximumParameterizedCount = 1000, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("BulkDeleteAsync", ActivityKind.Internal);

            if (entities == null || !entities.Any())
                throw new ArgumentNullException("entities");

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            if (!columns.Any(x => x.IsPrimaryKey))
                throw new System.Data.MissingPrimaryKeyException("should have a primary key.");

            var whereColumns = columns.Where(x => (x.IsPrimaryKey || x.IsVersion) && !x.Ignore).ToArray();

            var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            var versionPolicy = AttributeUtil.Find<VersionPolicyAttribute>(type);

            var chunkSize = Math.Max(maximumParameterizedCount / whereColumns.Length, 1);
            foreach (var group in entities.Chunk(chunkSize))
            {
                var items = group.ToArray();
                var dic = new Dictionary<string, object>();
                var sqlList = new List<string>();
                for (int i = 0; i < items.Length; i++)
                {
                    var entity = items[i];
                    foreach (var validator in validators)
                    {
                        var valdateError = validator.GetError(entity);
                        if (valdateError != null)
                        {
                            throw valdateError;
                        }
                    }

                    var whereList = new List<SetClausesHolder>();
                    foreach (var column in whereColumns)
                    {
                        var value = accessors[column.PropertyInfoName].GetValue(entity);
                        var whereClauses = new SetClausesHolder(column.Name, value, i);
                        dic[whereClauses.Placeholder] = whereClauses.Value;
                        whereList.Add(whereClauses);
                    }
                    var sql = string.Format("DELETE FROM {0} WHERE {1} ;", table,
                                string.Join(" AND ", whereList.Select(x => x.Clauses)));
                    sqlList.Add(sql);
                }
                var count = await cnn.ExecuteAsync(String.Join(Environment.NewLine, sqlList), dic, transaction, commandTimeout);
                if (count != items.Length)
                    throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");
            }
        }

        #endregion

        #region TruncateAsync

        async public static Task TruncateAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var type = typeof(T);
            var table = type.GetTableName().EscapeAliasFormat();
            await cnn.ExecuteAsync($"TRUNCATE TABLE {table}", null, transaction, commandTimeout);
        }

        #endregion

        #region DropTableAsync

        async public static Task DropIfExistsTableAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            var type = typeof(T);
            var table = type.GetTableName().EscapeAliasFormat();
            await cnn.ExecuteAsync($"DROP TABLE IF EXISTS {table}", null, transaction, commandTimeout);
        }

        #endregion
    }
}
