using System;
using System.Collections.Generic;
using System.Text;

namespace DapperExtensions.Sql
{
    public class SqliteDialect : SqlDialectBase
    {
        public override string GetIdentitySql(string tableName) => "SELECT LAST_INSERT_ROWID() AS [Id]";
        
        public override string GetPagingSql(string sql, int page, int resultsPerPage, IDictionary<string, object> parameters) =>
            GetSetSql(sql, page * resultsPerPage, resultsPerPage, parameters);
        
        public override string GetSetSql(string sql, int firstResult, int maxResults, IDictionary<string, object> parameters)
        {
            if (string.IsNullOrEmpty(sql)) throw new ArgumentNullException(nameof(sql));
            
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));
            
            parameters.Add("@Offset", firstResult);
            parameters.Add("@Count", maxResults);

            return $@"{sql} LIMIT @Offset, @Count";
        }

        public override string GetColumnName(string prefix, string columnName, string alias)
        {
            if (string.IsNullOrWhiteSpace(columnName))
                throw new ArgumentNullException(nameof(columnName), "Cannot be null or empty.");

            return string.IsNullOrWhiteSpace(alias) ? columnName : $"{columnName} AS {QuoteString(alias)}";
        }
    }
}
