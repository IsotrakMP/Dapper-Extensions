using System;
using System.Collections.Generic;
using System.Data;
using DapperExtensions.Mapper;
using DapperExtensions.Sql;

namespace DapperExtensions
{
    public class Database : IDatabase
    {
        private readonly IDapperImplementor dapper;

        private IDbTransaction Transaction { get; set; }

        public Database(IDbConnection connection, ISqlGenerator sqlGenerator)
        {
            dapper = new DapperImplementor(sqlGenerator);
            Connection = connection;
            
            if (Connection.State != ConnectionState.Open)
            {
                Connection.Open();
            }
        }

        public bool HasActiveTransaction => Transaction != null;

        public IDbConnection Connection { get; }

        public void Dispose()
        {
            if (Connection.State != ConnectionState.Closed)
            {
                Transaction?.Rollback();
                Connection.Close();
            }
        }

        public void BeginTransaction(IsolationLevel isolationLevel = IsolationLevel.ReadCommitted) => Transaction = Connection.BeginTransaction(isolationLevel);
        
        public void Commit()
        {
            Transaction.Commit();
            Transaction = null;
        }

        public void Rollback()
        {
            Transaction.Rollback();
            Transaction = null;
        }

        public void RunInTransaction(Action action)
        {
            BeginTransaction();

            try
            {
                action();
                Commit();
            }
            catch (Exception ex)
            {
                if (HasActiveTransaction)
                {
                    Rollback();
                }

                throw ex;
            }
        }

        public T RunInTransaction<T>(Func<T> func)
        {
            BeginTransaction();
            try
            {
                T result = func();
                Commit();
                return result;
            }
            catch (Exception ex)
            {
                if (HasActiveTransaction)
                {
                    Rollback();
                }

                throw ex;
            }
        }
        
        public T Get<T>(dynamic id, IDbTransaction transaction, int? commandTimeout)
            where T : class
        {
            return (T)dapper.Get<T>(Connection, id, transaction, commandTimeout);
        }

        public T Get<T>(dynamic id, int? commandTimeout)
            where T : class
        {
            return (T)dapper.Get<T>(Connection, id, Transaction, commandTimeout);
        }

        public void Insert<T>(IEnumerable<T> entities, IDbTransaction transaction, int? commandTimeout)
            where T : class
        {
            dapper.Insert<T>(Connection, entities, transaction, commandTimeout);
        }

        public void Insert<T>(IEnumerable<T> entities, int? commandTimeout)
            where T : class
        {
            dapper.Insert<T>(Connection, entities, Transaction, commandTimeout);
        }

        public dynamic Insert<T>(T entity, IDbTransaction transaction, int? commandTimeout)
            where T : class
        {
            return dapper.Insert<T>(Connection, entity, transaction, commandTimeout);
        }

        public dynamic Insert<T>(T entity, int? commandTimeout)
            where T : class
        {
            return dapper.Insert<T>(Connection, entity, Transaction, commandTimeout);
        }

        public bool Update<T>(T entity, IDbTransaction transaction, int? commandTimeout)
            where T : class
        {
            return dapper.Update<T>(Connection, entity, transaction, commandTimeout);
        }

        public bool Update<T>(T entity, int? commandTimeout)
            where T : class
        {
            return dapper.Update<T>(Connection, entity, Transaction, commandTimeout);
        }

        public bool Delete<T>(T entity, IDbTransaction transaction, int? commandTimeout)
            where T : class
        {
            return dapper.Delete(Connection, entity, transaction, commandTimeout);
        }

        public bool Delete<T>(T entity, int? commandTimeout)
            where T : class
        {
            return dapper.Delete(Connection, entity, Transaction, commandTimeout);
        }

        public bool Delete<T>(object predicate, IDbTransaction transaction, int? commandTimeout) 
            where T : class
        {
            return dapper.Delete<T>(Connection, predicate, transaction, commandTimeout);
        }

        public bool Delete<T>(object predicate, int? commandTimeout)
            where T : class
        {
            return dapper.Delete<T>(Connection, predicate, Transaction, commandTimeout);
        }

        public IEnumerable<T> GetList<T>(object predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout, bool buffered)
            where T : class
        {
            return dapper.GetList<T>(Connection, predicate, sort, transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetList<T>(object predicate, IList<ISort> sort, int? commandTimeout, bool buffered)
            where T : class
        {
            return dapper.GetList<T>(Connection, predicate, sort, Transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetPage<T>(object predicate, IList<ISort> sort, int page, int resultsPerPage, IDbTransaction transaction, int? commandTimeout, bool buffered)
            where T : class
        {
            return dapper.GetPage<T>(Connection, predicate, sort, page, resultsPerPage, transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetPage<T>(object predicate, IList<ISort> sort, int page, int resultsPerPage, int? commandTimeout, bool buffered)
            where T : class
        {
            return dapper.GetPage<T>(Connection, predicate, sort, page, resultsPerPage, Transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetSet<T>(object predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction, int? commandTimeout, bool buffered)
            where T : class
        {
            return dapper.GetSet<T>(Connection, predicate, sort, firstResult, maxResults, transaction, commandTimeout, buffered);
        }

        public IEnumerable<T> GetSet<T>(object predicate, IList<ISort> sort, int firstResult, int maxResults, int? commandTimeout, bool buffered)
            where T : class
        {
            return dapper.GetSet<T>(Connection, predicate, sort, firstResult, maxResults, Transaction, commandTimeout, buffered);
        }

        public int Count<T>(object predicate, IDbTransaction transaction, int? commandTimeout)
            where T : class
        {
            return dapper.Count<T>(Connection, predicate, transaction, commandTimeout);
        }

        public int Count<T>(object predicate, int? commandTimeout)
            where T : class
        {
            return dapper.Count<T>(Connection, predicate, Transaction, commandTimeout);
        }

        public IMultipleResultReader GetMultiple(GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout)
        {
            return dapper.GetMultiple(Connection, predicate, transaction, commandTimeout);
        }

        public IMultipleResultReader GetMultiple(GetMultiplePredicate predicate, int? commandTimeout)
        {
            return dapper.GetMultiple(Connection, predicate, Transaction, commandTimeout);
        }

        public void ClearCache()
        {
            dapper.SqlGenerator.Configuration.ClearCache();
        }

        public Guid GetNextGuid()
        {
            return dapper.SqlGenerator.Configuration.GetNextGuid();
        }

        public IClassMapper GetMap<T>()
            where T : class
        {
            return dapper.SqlGenerator.Configuration.GetMap<T>();
        }
    }
}