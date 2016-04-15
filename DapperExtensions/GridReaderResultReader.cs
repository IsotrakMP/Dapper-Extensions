using System.Collections.Generic;
using Dapper;

namespace DapperExtensions
{
    public class GridReaderResultReader : IMultipleResultReader
    {
        private readonly SqlMapper.GridReader reader;

        public GridReaderResultReader(SqlMapper.GridReader reader)
        {
            this.reader = reader;
        }

        public IEnumerable<T> Read<T>() => reader.Read<T>();
    }
}