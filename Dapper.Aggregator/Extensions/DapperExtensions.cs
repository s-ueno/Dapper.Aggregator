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

        #region FindIdAsync

        async public static Task<T> FindIdAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null, params object[] keys)
        {
            var query = new Query<T>().PrimaryKeyFilter(keys);
            return (await cnn.FindAsync(query, transaction)).FirstOrDefault();
        }

        #endregion

        #region FindAsync

        async public static Task<IEnumerable<T>> FindAsync<T>(this IDbConnection cnn, Query<T> query, IDbTransaction transaction = null)
        {
            using var activity = _activitySource.StartActivity("FindAsync", ActivityKind.Internal);
            activity?.AddTag("Sql", query.Sql);

            query.Ensure();

            var oldType = query.RootType;
            var newParentType = ILGeneratorUtil.IsInjected(oldType) ? ILGeneratorUtil.InjectionInterfaceWithProperty(oldType) : oldType;

            var rows = await cnn.QueryAsync(newParentType, query.Sql, query.Parameters, transaction, query.Timeout);
            if (rows != null && rows.Any())
            {
                var command = new CommandDefinition(query.SqlIgnoreOrderBy, query.Parameters, transaction, query.Timeout);
                await LoadAsync(cnn, command, newParentType, query.Relations.ToArray(), rows);
            }

            if (activity?.IsAllDataRequested == true)
            {
                activity?.AddTag("Result Count", rows.Count());
            }

            return rows.OfType<T>();
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

        async public static Task<T> ScalarAsync<T>(this IDbConnection cnn, QueryImp query, IDbTransaction transaction = null)
        {
            using var activity = _activitySource.StartActivity("ScalarWith", ActivityKind.Internal);
            activity?.AddTag("Sql", query.Sql);

            var result = await cnn.ExecuteScalarAsync<T>(query.Sql, query.Parameters, transaction, query.Timeout, CommandType.Text);

            activity?.AddTag("Result", result);

            return result;
        }

        #endregion

        #region DeleteQueryAsync

        async public static Task<int> DeleteQueryAsync(this IDbConnection cnn, QueryImp query,
            IDbTransaction transaction = null, bool isRootOnly = true)
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
                    cnn.Execute(each.DeleteClause, query.Parameters, transaction, query.Timeout);
                }
            }
            var rootDeleteSql = string.Format("DELETE FROM {0} {1}", query.TableClause, query.WhereClause);
            var effect = await cnn.ExecuteAsync(rootDeleteSql, query.Parameters, transaction, query.Timeout);

            activity?.AddTag("Effect", effect);

            return effect;
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

        async public static Task<int> UpdateQueryAsync<T>(this IDbConnection cnn, UpdateQuery<T> query, IDbTransaction transaction = null)
        {
            using var activity = _activitySource.StartActivity("UpdateQuery", ActivityKind.Internal);
            activity?.AddTag("Sql", query.UpdateClauses);

            var effect = await cnn.ExecuteAsync(query.UpdateClauses, query.Parameters, transaction, query.Timeout);

            activity?.AddTag("Effect", effect);

            return effect;
        }

        #endregion

        #region CreateIfNotExistsTableAsync

        async public static Task<int> CreateIfNotExistsTableAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null)
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

            return await cnn.ExecuteAsync(sb.ToString(), null, transaction);
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

        async public static Task<int> CountAsync<T>(this IDbConnection cnn, Query<T> query, IDbTransaction transaction = null)
        {
            var sql = string.Format("SELECT COUNT(1) FROM ({0}) T", query.SqlIgnoreOrderBy);

            using var activity = _activitySource.StartActivity("Count", ActivityKind.Internal);
            activity?.AddTag("Sql", sql);

            var count = await cnn.ExecuteScalarAsync<int>(sql, query.Parameters, transaction, query.Timeout);

            activity?.AddTag("Count", count);

            return count;
        }

        async public static Task<TResult> CountAsync<TResult>(this IDbConnection cnn, QueryImp query, IDbTransaction transaction = null)
        {
            var sql = string.Format("SELECT COUNT(1) FROM ({0}) T", query.SqlIgnoreOrderBy);

            using var activity = _activitySource.StartActivity("Count", ActivityKind.Internal);
            activity?.AddTag("Sql", sql);

            var count = await cnn.ExecuteScalarAsync<TResult>(sql, query.Parameters, transaction, query.Timeout);

            activity?.AddTag("Count", count);

            return count;
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
            var type = typeof(T);
            var table = type.GetTableName();

            return await BulkInsertRawAsync<T>(cnn, entities, transaction, maximumParameterizedCount, commandTimeout, table);
        }

        async internal static Task<int> BulkInsertRawAsync<T>(this IDbConnection cnn, IEnumerable<T> entities,
                    IDbTransaction transaction, int maximumParameterizedCount, int? commandTimeout, string tableName, bool ignoreUpdateVersion = false)
        {
            using var activity = _activitySource.StartActivity("BulkInsertAsync", ActivityKind.Internal);

            if (entities == null)
                throw new ArgumentNullException("entities");

            if (!entities.Any())
            {
                activity.AddTag("entities", "no data");
                return 0;
            }

            var type = typeof(T);
            var table = tableName.EscapeAliasFormat();
            var columns = type.GetSelectClause().ToArray();
            var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            var versionPolicy = AttributeUtil.Find<VersionPolicyAttribute>(type);
            if (!versionPolicy.Any()) versionPolicy = new[] { new DefalutVersionPolicy() };


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
                        var validateError = validator.GetError(entity, PersistState.Insert);
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
                        if (column.IsVersion && !ignoreUpdateVersion)
                        {
                            foreach (var policy in versionPolicy)
                            {
                                value = policy.Generate(value);
                                if (accessors[column.PropertyInfoName].CanWrite)
                                {
                                    accessors[column.PropertyInfoName].SetValue(entity, value);
                                }
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

            activity?.AddTag("Effect", result);

            return result;
        }


        #endregion

        #region UpdateEntityAsync / BulkUpdateAsync

        async public static Task UpdateEntityAsync<T>(this IDbConnection cnn, T entity,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("UpdateEntityAsync", ActivityKind.Internal);

            if (entity == null)
                throw new ArgumentNullException("entity");

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            if (!columns.Any(x => x.IsPrimaryKey))
                throw new System.Data.MissingPrimaryKeyException("should have a primary key.");

            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            foreach (var validator in validators)
            {
                var validateError = validator.GetError(entity, PersistState.Update);
                if (validateError != null)
                {
                    throw validateError;
                }
            }

            var dic = new Dictionary<string, object>();
            var index = 0;

            var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
            var whereColumns = columns.Where(x => (x.IsPrimaryKey || x.IsVersion) && !x.Ignore).ToArray();
            var whereClauses = new List<string>();
            foreach (var each in whereColumns)
            {
                var value = accessors[each.PropertyInfoName].GetValue(entity);
                var clauses = new SetClausesHolder(each.Name, value, index);
                dic[clauses.Placeholder] = clauses.Value;
                whereClauses.Add(clauses.Clauses);
            }


            var versionColumns = columns.Where(x => x.IsVersion).ToArray();
            var versionPolicy = AttributeUtil.Find<VersionPolicyAttribute>(type);
            if (!versionPolicy.Any()) versionPolicy = new[] { new DefalutVersionPolicy() };            
            foreach (var column in versionColumns)
            {
                var value = accessors[column.PropertyInfoName].GetValue(entity);
                foreach (var policy in versionPolicy)
                {
                    value = policy.Generate(value);
                    if (accessors[column.PropertyInfoName].CanWrite)
                    {
                        accessors[column.PropertyInfoName].SetValue(entity, value);
                    }
                }
            }

            index += 1;
            var setColumns = columns.Where(x => !x.IsPrimaryKey && !x.Ignore).ToArray();
            var setClauses = new List<string>();
            foreach (var each in setColumns)
            {
                var value = accessors[each.PropertyInfoName].GetValue(entity);
                var clauses = new SetClausesHolder(each.Name, value, index);
                dic[clauses.Placeholder] = clauses.Value;
                setClauses.Add(clauses.Clauses);
            }

            var sql = $"UPDATE {table} SET {string.Join(",", setClauses)} WHERE {String.Join(" AND ", whereClauses)} ;";

            activity.AddTag("Sql", sql);

            var ret = await cnn.ExecuteAsync(sql, dic, transaction, commandTimeout);
            if (ret != 1)
                throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");

        }

        async public static Task BulkUpdateAsync<T>(this IDbConnection cnn, IEnumerable<T> entities,
            IDbTransaction transaction = null, int maximumParameterizedCount = 1000, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("BulkUpdateAsync", ActivityKind.Internal);

            if (entities == null)
                throw new ArgumentNullException("entities");

            if (!entities.Any())
            {
                activity.AddTag("entities", "no data");
                return;
            }

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            if (!columns.Any(x => x.IsPrimaryKey))
                throw new System.Data.MissingPrimaryKeyException("should have a primary key.");
            if (!columns.All(x =>
                !String.IsNullOrWhiteSpace(x.Name) &&
                !String.IsNullOrWhiteSpace(x.DDLType)
            ))
            {
                throw new InvalidOperationException("Add Name and DDLType to the ColumnAttribute.");
            }

            var whereColumns = columns.Where(x => (x.IsPrimaryKey || x.IsVersion) && !x.Ignore).ToArray();
            var setColumns = columns.Where(x => !x.IsPrimaryKey && !x.Ignore).ToArray();

            // CREATE TABLE TABLE
            var tempTableName = $"t_{Guid.NewGuid().ToString().Replace('-', '_').ToLower()}";
            var ddlColumns = columns.Select(x => toDDLColumn(x));
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TEMP TABLE {tempTableName}(");
            sb.AppendLine($"{String.Join(",", ddlColumns)}");
            sb.AppendLine($")");
            await cnn.ExecuteAsync(sb.ToString(), null, transaction, commandTimeout);

            // BULK INSERT TO TEMP TABLE
            await BulkInsertRawAsync<T>(cnn, entities, transaction, maximumParameterizedCount, commandTimeout, tempTableName, true);

            // transfer table from temptable
            var whereClauses = whereColumns.Select(x => $"{table}.{x.Name} = {tempTableName}.{x.Name}");
            var setClauses = setColumns.Select(x => $"{x.Name} = {tempTableName}.{x.Name}");
            var transferSql = $"UPDATE {table} SET {string.Join(",", setClauses)} FROM {tempTableName} where {string.Join(" AND ", whereClauses)}";

            var ret = await cnn.ExecuteAsync(transferSql, null, transaction, commandTimeout);
            if (ret != entities.Count())
                throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");

            activity?.AddTag("Effect", ret);


            // update version check
            var versionPolicy = AttributeUtil.Find<VersionPolicyAttribute>(type);
            if (!versionPolicy.Any()) versionPolicy = new[] { new DefalutVersionPolicy() };

            var versionColumns = columns.Where(x => x.IsVersion).ToArray();
            object maxVersion = null;
            if (versionColumns.Any() && versionPolicy != null)
            {
                var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
                foreach (var entity in entities)
                {
                    foreach (var column in versionColumns)
                    {
                        var value = accessors[column.PropertyInfoName].GetValue(entity);
                        foreach (var policy in versionPolicy)
                        {
                            value = policy.Generate(value);
                            if (accessors[column.PropertyInfoName].CanWrite)
                            {
                                accessors[column.PropertyInfoName].SetValue(entity, value);
                            }
                        }

                        if (maxVersion == null)
                        {
                            maxVersion = value;
                        }
                        else if (value != null)
                        {
                            var list = new List<object>();
                            list.Add(maxVersion);
                            list.Add(value);
                            list.Sort();
                            maxVersion = list.LastOrDefault();
                        }
                    }
                }

                var vesionCase = versionColumns.Select(x => $"{x.Name} = @p1");
                var versionUpSql = $"UPDATE {table} SET {string.Join(",", vesionCase)} WHERE EXISTS(SELECT 1 FROM {tempTableName} WHERE {string.Join(" AND ", whereClauses)})";
                ret = await cnn.ExecuteAsync(versionUpSql, new Dictionary<string, object>() { { "@p1", maxVersion } }, transaction, commandTimeout);
                if (ret != entities.Count())
                    throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");

                activity?.AddTag("Effect", ret);
            }

            // drop temp table
            await cnn.ExecuteAsync($"DROP TABLE {tempTableName}", null, transaction, commandTimeout);
        }

        #endregion

        #region DeleteEntityAsync / BulkDeleteAsync

        async public static Task DeleteEntityAsync<T>(this IDbConnection cnn, T entity,
            IDbTransaction transaction = null, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("DeleteEntityAsync", ActivityKind.Internal);

            if (entity == null)
                throw new ArgumentNullException("entity");

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            if (!columns.Any(x => x.IsPrimaryKey))
                throw new System.Data.MissingPrimaryKeyException("should have a primary key.");

            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            foreach (var validator in validators)
            {
                var validateError = validator.GetError(entity, PersistState.Delete);
                if (validateError != null)
                {
                    throw validateError;
                }
            }

            var dic = new Dictionary<string, object>();
            var accessors = PropertyAccessorImp.ToPropertyAccessors(type).ToDictionary(x => x.Name);
            var whereColumns = columns.Where(x => (x.IsPrimaryKey || x.IsVersion) && !x.Ignore).ToArray();
            var whereClauses = new List<string>();
            foreach (var each in whereColumns)
            {
                var value = accessors[each.PropertyInfoName].GetValue(entity);
                var clauses = new SetClausesHolder(each.Name, value, 0);
                dic[clauses.Placeholder] = clauses.Value;
                whereClauses.Add(clauses.Clauses);
            }

            var sql = $"DELETE FROM {table} WHERE {String.Join(" AND ", whereClauses)} ;";

            activity.AddTag("Sql", sql);

            var ret = await cnn.ExecuteAsync(sql, dic, transaction, commandTimeout);
            if (ret != 1)
                throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");

        }

        async public static Task BulkDeleteAsync<T>(this IDbConnection cnn, IEnumerable<T> entities,
           IDbTransaction transaction = null, int maximumParameterizedCount = 1000, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("BulkDeleteAsync", ActivityKind.Internal);

            if (entities == null)
                throw new ArgumentNullException("entities");

            if (!entities.Any())
            {
                activity.AddTag("entities", "no data");
                return;
            }

            var type = typeof(T);
            var table = type.GetTableName();
            var columns = type.GetSelectClause().ToArray();
            if (!columns.Any(x => x.IsPrimaryKey))
                throw new System.Data.MissingPrimaryKeyException("should have a primary key.");
            if (!columns.All(x =>
                !String.IsNullOrWhiteSpace(x.Name) &&
                !String.IsNullOrWhiteSpace(x.DDLType)
            ))
            {
                throw new InvalidOperationException("Add Name and DDLType to the ColumnAttribute.");
            }

            var validators = AttributeUtil.Find<EntityValidateAttribute>(type);
            foreach (var entity in entities)
            {
                foreach (var validator in validators)
                {
                    var validateError = validator.GetError(entity, PersistState.Delete);
                    if (validateError != null)
                    {
                        throw validateError;
                    }
                }
            }

            // CREATE TABLE TABLE
            var tempTableName = $"t_{Guid.NewGuid().ToString().Replace('-', '_').ToLower()}";
            var ddlColumns = columns.Select(x => toDDLColumn(x));
            var sb = new StringBuilder();
            sb.AppendLine($"CREATE TEMP TABLE {tempTableName}(");
            sb.AppendLine($"{String.Join(",", ddlColumns)}");
            sb.AppendLine($")");
            await cnn.ExecuteAsync(sb.ToString(), null, transaction, commandTimeout);

            // BULK INSERT TO TEMP TABLE
            await BulkInsertRawAsync<T>(cnn, entities, transaction, maximumParameterizedCount, commandTimeout, tempTableName, true);

            // 
            var whereColumns = columns.Where(x => (x.IsPrimaryKey || x.IsVersion) && !x.Ignore).ToArray();
            var whereClauses = whereColumns.Select(x => $"{table}.{x.Name} = {tempTableName}.{x.Name}");

            var deleteSql = $"DELETE FROM {table} WHERE EXISTS(SELECT 1 FROM {tempTableName} WHERE {string.Join(" AND ", whereClauses)})";
            var ret = await cnn.ExecuteAsync(deleteSql, null, transaction, commandTimeout);
            if (ret != entities.Count())
                throw new System.Data.DBConcurrencyException("entity has already been updated by another user.");

            activity?.AddTag("Effect", ret);

            // drop temp table
            await cnn.ExecuteAsync($"DROP TABLE {tempTableName}", null, transaction, commandTimeout);
        }

        #endregion

        #region TruncateAsync

        async public static Task TruncateAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("TruncateAsync", ActivityKind.Internal);

            var type = typeof(T);
            var table = type.GetTableName().EscapeAliasFormat();
            await cnn.ExecuteAsync($"TRUNCATE TABLE {table}", null, transaction, commandTimeout);
        }

        #endregion

        #region DropTableAsync

        async public static Task DropIfExistsTableAsync<T>(this IDbConnection cnn, IDbTransaction transaction = null, int? commandTimeout = null)
        {
            using var activity = _activitySource.StartActivity("DropIfExistsTableAsync", ActivityKind.Internal);

            var type = typeof(T);
            var table = type.GetTableName().EscapeAliasFormat();
            await cnn.ExecuteAsync($"DROP TABLE IF EXISTS {table}", null, transaction, commandTimeout);
        }

        #endregion

        #region Table Lock

        async public static Task TableLock<T>(this IDbConnection cnn, IDbTransaction transaction = null, int? commandTimeout = 30 * 1000)
        {
            using var activity = _activitySource.StartActivity("TableLock", ActivityKind.Internal);
            var type = typeof(T);
            var table = type.GetTableName().EscapeAliasFormat();
            await cnn.ExecuteAsync($"LOCK TABLE {table} ;", null, transaction, commandTimeout);
        }

        #endregion

    }
}
