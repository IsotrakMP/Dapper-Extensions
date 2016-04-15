using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DapperExtensions.Sql
{
    public class PostgreSqlDialect : SqlDialectBase
    {
        public override string GetIdentitySql(string tableName) => "SELECT LASTVAL() AS Id";

        public override string GetPagingSql(string sql, int page, int resultsPerPage, IDictionary<string, object> parameters) =>
            GetSetSql(sql, page * resultsPerPage, resultsPerPage, parameters);
        
        public override string GetSetSql(string sql, int firstResult, int maxResults, IDictionary<string, object> parameters)
        {
            parameters.Add("@firstResult", firstResult);
            parameters.Add("@maxResults", maxResults);

            return $@"{sql} LIMIT @firstResult OFFSET @pageStartRowNbr";
        }

        public override string GetColumnName(string prefix, string columnName, string alias) =>
            base.GetColumnName(null, columnName, alias).ToLower();
        
        public override string GetTableName(string schemaName, string tableName, string alias) =>
            base.GetTableName(schemaName, tableName, alias).ToLower();        
    }

}