using EduDBCore;
using System;
using System.Collections.Generic;
using System.Linq;

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
        internal bool UpdateValueInColumm(string columnName, object newValue)
        {
            if (items[columnName].Value != null)
            {
                items[columnName].Value = newValue;
                return true;
            }
            return false;
        }

        internal IntSchValue GetValueInColumn(string columnName)
        {
            return items[columnName];
        }

        internal bool RemoveColumn(string columnName)
        {
            return items.Remove(columnName);
        }

        public override string ToString()
        {
            string result = "";

            foreach(KeyValuePair<string, IntSchValue> entry in items)
            {
                result += entry.Value.ToString();
            }

            return result;
        }

        public List<string> GetColumnNames()
        {
            return items.Keys.ToList();
        }
    }
}