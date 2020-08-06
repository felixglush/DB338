using System;
using System.Collections.Generic;
using System.Text;

namespace DB338Core
{
    class IntSchTable
    {

        private string name;

        private List<IntSchColumn> columns;
        private List<string> columnNames;

        public IntSchTable(string initname)
        {
            name = initname;
            columns = new List<IntSchColumn>();
            columnNames = new List<string>();
        }

        public string Name { get => name; set => name = value; }


        public string[,] Select(List<string> cols)
        {
            string[,] results = new string[columns[0].items.Count, cols.Count];

            for (int i = 0; i < cols.Count; ++i)
            {
                for (int j = 0; j < columns.Count; ++j)
                {
                    if (cols[i] == columns[j].Name)
                    {
                        for (int z = 0; z < columns[0].items.Count; ++z)
                        {
                            results[z, i] = columns[j].items[z];
                        }
                    }
                }
            }

            return results;
        }

        public bool Project()
        {
            throw new NotImplementedException();
        }

        public void Insert(List<string> cols, List<string> vals)
        {
            for (int i = 0; i < cols.Count; ++i)
            {
                columnNames.Add(columns[i].Name);

                for (int j = 0; j < columns.Count; ++j)
                {
                    if (columns[j].Name == cols[i])
                    {
                        columns[j].items.Add(vals[i]);
                    }
                }
            }
        }

        public bool AddColumn(string name, string type)
        {
            foreach (IntSchColumn col in columns)
            {
                if (col.Name == name)
                {
                    return false;
                }
            }

            columns.Add(new IntSchColumn(name, type));
            return true;
        }

        public string[,] Update(List<string> updateCols, List<string> values, string conditionCol, string conditionValue, string condition)
        {
            // return the string[,] of the table affected
            return Select(getColumnNames());
        }
        
        private List<string> getColumnNames()
        {
            List<string> names = new List<string>();
            foreach (IntSchColumn col in columns)
            {
                names.Add(col.Name);
            }
            return names;
        }

        internal string[,] addColumns(IDictionary<string, string> columns)
        {
            throw new NotImplementedException();
        }

        internal string[,] dropColumn(string columnName)
        {
            throw new NotImplementedException();
        }
    }
}
