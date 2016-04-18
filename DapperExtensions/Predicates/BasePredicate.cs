using System;
using System.Collections.Generic;
using System.Linq;
using DapperExtensions.Sql;

namespace DapperExtensions
{
    public abstract class BasePredicate : IBasePredicate
    {
        public abstract string GetSql(ISqlGenerator sqlGenerator, IDictionary<string, object> parameters);
        public string PropertyName { get; set; }

        protected virtual string GetColumnName(Type entityType, ISqlGenerator sqlGenerator, string propertyName)
        {
            var map = sqlGenerator.Configuration.GetMap(entityType);
            if (map == null)
            {
                throw new NullReferenceException($"Map was not found for {entityType}");
            }

            var propertyMap = map.Properties.SingleOrDefault(p => p.Name == propertyName);
            if (propertyMap == null)
            {
                throw new NullReferenceException($"{propertyName} was not found for {entityType}");
            }

            return sqlGenerator.GetColumnName(map, propertyMap, false);
        }
    }
}
