using System.Collections.Generic;
using DapperExtensions.Sql;

namespace DapperExtensions
{
    public interface IPredicate
    {
        string GetSql(ISqlGenerator sqlGenerator, IDictionary<string, object> parameters);
    }
}
