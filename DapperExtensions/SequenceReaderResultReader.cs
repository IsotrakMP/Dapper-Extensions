﻿using System.Collections.Generic;
using Dapper;

namespace DapperExtensions
{
    public class SequenceReaderResultReader : IMultipleResultReader
    {
        private readonly Queue<SqlMapper.GridReader> _items;

        public SequenceReaderResultReader(IEnumerable<SqlMapper.GridReader> items)
        {
            this._items = new Queue<SqlMapper.GridReader>(items);
        }

        public IEnumerable<T> Read<T>()
        {
            SqlMapper.GridReader reader = _items.Dequeue();
            return reader.Read<T>();
        }
    }
}
