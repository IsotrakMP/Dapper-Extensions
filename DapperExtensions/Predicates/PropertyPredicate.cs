using System.Collections.Generic;
using DapperExtensions.Sql;

namespace DapperExtensions
{
    public class PropertyPredicate<T, T2> : ComparePredicate, IPropertyPredicate
        where T : class
        where T2 : class
    {
        public string PropertyName2 { get; set; }

        public override string GetSql(ISqlGenerator sqlGenerator, IDictionary<string, object> parameters) =>
            $"({GetColumnName(typeof(T), sqlGenerator, PropertyName)} {GetOperatorString()} {GetColumnName(typeof(T2), sqlGenerator, PropertyName2)})";
    }
}
