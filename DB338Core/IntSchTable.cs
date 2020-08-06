using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;

namespace DB338Core
{
    class IntSchTable
    {

        private string name;
        int numRows;
        private IDictionary<string, IntSchColumn> columns;


        private int LOG = 1; // maybe make a logger class?

        public IntSchTable(string initname)
        {
            name = initname;
            numRows = 0;
            columns = new Dictionary<string, IntSchColumn>();
        }

        public string Name { get => name; set => name = value; }
        
        private void printList(List<string> list, string type)
        {
            if (LOG == 1) {
                System.Console.WriteLine(type);
                list.ForEach(System.Console.WriteLine);
            }
        }

        public string[,] Select(List<string> cols, List<string> whereClause)
        {
            if (whereClause != null && whereClause.Count > 0)
            {
                string conditionOn = "";
                string conditionOperator = "";
                string condition = "";

            }

            printList(cols, "SELECT COLUMNS");
            printList(whereClause, "WHERE CLAUSE");

            // numRows x number of selected cols
            string[,] results = new string[numRows, cols.Count];

            // go across the selected columns
            for (int i = 0; i < cols.Count; ++i)
            {
                string name = cols[i];
                IntSchColumn theCol = columns[name];
                    
                // go through items in this column and append to the results
                for (int row = 0; row < numRows; ++row)
                {
                    results[row, i] = theCol.Get(row);
                }
            }

            return results;
        }

        public bool Project()
        {
            throw new NotImplementedException();
        }

        // insert a record into the table
        public void Insert(List<string> columnNames, List<string> columnValues)
        {
            printList(columnNames, "insert column names");
            printList(columnValues, "insert column values");

            if (columnNames.Count == columnValues.Count)
            {
                for (int i = 0; i < columnNames.Count; ++i)
                {
                    columns[columnNames[i]].items.Add(columnValues[i]);
                    numRows += 1;
                }
            }
        }

        public bool AddColumn(string name, string type)
        {
            if (columns.ContainsKey(name)) return false;

            if (LOG == 1) System.Console.WriteLine("ADD COLUMN " + name + " " + type);

            columns.Add(name, new IntSchColumn(name, type));

            return true;
        }

        // Dictionary maps column names to types
        internal bool addColumns(IDictionary<string, string> columns)
        {
            bool result = false;
            foreach (KeyValuePair<string, string> entry in columns)
            {
                result = AddColumn(entry.Key, entry.Value);
                if (!result) return false;
            }

            return result;
        }

        public bool dropColumn(string columnName) => columns.Remove(columnName);

        public string[,] Update(List<string> updateCols, List<string> values, string conditionCol, string conditionValue, string condition)
        {
            // return the string[,] of the table affected
            return Select((List<string>) columns.Keys, null);
        }

       
    }
}
