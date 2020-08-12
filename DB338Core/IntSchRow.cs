using EduDBCore;
using System;
using System.Collections.Generic;

namespace DB338Core
{ 
    internal class IntSchRow
    {
        private Dictionary<string, IntSchValue> items;

        public IntSchRow()
        {
            items = new Dictionary<string, IntSchValue>();
        }

        // this puts a new IntSchValue into the column
        internal void SetValueInColumn(string columnName, IntSchValue intSchValue)
        {
            items[columnName] = intSchValue;
        }

        // this updates an existing IntSchValue in the column
        internal void UpdateValueInColumm(string columnName, object newValue)
        {
            items[columnName].Value = newValue; 
        }

        internal IntSchValue GetValueInColumn(string columnName)
        {
            return items[columnName];
        }

        internal bool RemoveColumn(string columnName)
        {
            return items.Remove(columnName);
        }
  
    }
}