using System.Collections.Generic;

namespace DapperExtensions.Sql
{
    public class MySqlDialect : SqlDialectBase
    {
        public override char OpenQuote => '`'; 
        
        public override char CloseQuote =>'`'; 
        
        public override string GetIdentitySql(string tableName) => "SELECT CONVERT(LAST_INSERT_ID(), SIGNED INTEGER) AS ID";
        
        public override string GetPagingSql(string sql, int page, int resultsPerPage, IDictionary<string, object> parameters) =>
            GetSetSql(sql, page * resultsPerPage, resultsPerPage, parameters);
        
        public override string GetSetSql(string sql, int firstResult, int maxResults, IDictionary<string, object> parameters)
        {            
            parameters.Add("@firstResult", firstResult);
            parameters.Add("@maxResults", maxResults);

            return $@"{sql} LIMIT @firstResult, @maxResults";
        }
    }
}