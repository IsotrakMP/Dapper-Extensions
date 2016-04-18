using System.Collections.Generic;
using Dapper;

namespace DapperExtensions
{
    public class GridReaderResultReader : IMultipleResultReader
    {
        private readonly SqlMapper.GridReader _reader;

        public GridReaderResultReader(SqlMapper.GridReader reader)
        {
            this._reader = reader;
        }

        public IEnumerable<T> Read<T>() => _reader.Read<T>();
    }
}