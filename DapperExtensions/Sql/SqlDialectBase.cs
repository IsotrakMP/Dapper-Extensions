using System;
using System.Collections.Generic;
using System.Linq;

namespace DapperExtensions.Sql
{
    public abstract class SqlDialectBase : ISqlDialect
    {
        public virtual char OpenQuote => '"'; 

        public virtual char CloseQuote => '"'; 

        public virtual string BatchSeperator => ";" + Environment.NewLine;

        public virtual bool SupportsMultipleStatements => true;

        public virtual char ParameterPrefix => '@';

        public string EmptyExpression => "1=1";

        public virtual string GetTableName(string schemaName, string tableName, string alias)
        {
            if (string.IsNullOrWhiteSpace(tableName)) throw new ArgumentNullException(nameof(tableName), "tableName cannot be null or empty.");

            var result = string.IsNullOrWhiteSpace(schemaName) ? QuoteString(tableName) : $"{QuoteString(schemaName)}.{QuoteString(tableName)}";
            
            if (!string.IsNullOrWhiteSpace(alias))
            {
                result += $" AS {QuoteString(alias)}";
            }

            return result;
        }

        public virtual string GetColumnName(string prefix, string columnName, string alias)
        {
            if (string.IsNullOrWhiteSpace(columnName)) throw new ArgumentNullException(nameof(columnName), "columnName cannot be null or empty.");

            var result = string.IsNullOrWhiteSpace(prefix) ? QuoteString(columnName) : $"{QuoteString(prefix)}.{QuoteString(columnName)}";

            if (!string.IsNullOrWhiteSpace(alias))
            {
                result += $" AS {QuoteString(alias)}";
            }

            return result;
        }

        public abstract string GetIdentitySql(string tableName);
        public abstract string GetPagingSql(string sql, int page, int resultsPerPage, IDictionary<string, object> parameters);
        public abstract string GetSetSql(string sql, int firstResult, int maxResults, IDictionary<string, object> parameters);

        public virtual bool IsQuoted(string value)
        {
            if (value.Trim()[0] == OpenQuote)
            {
                return value.Trim().Last() == CloseQuote;
            }

            return false;
        }

        public virtual string QuoteString(string value) => IsQuoted(value) ? value : $"{OpenQuote}{value.Trim()}{CloseQuote}";

        public virtual string UnQuoteString(string value) => IsQuoted(value) ? value.Substring(1, value.Length - 2) : value;
    }
}