using EduDBCore;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;
using System.Linq;
using System.Collections;

namespace DB338Core
{
    class IntSchTable
    {
        // tradeoff between O(1) row access and O(1) column access...


        private string name;
        int numRows;
        private IDictionary<string, IntSchColumn> columns;

        // list of records, each record is a map between column name and the value. this makes filtering out entire rows easier.
        // otherwise, would have to create a list of indices for which rows to filter out when going through the columns variable
        private List<IntSchRow> rows;

        private int LOG = 1; // maybe make a logger class?

        public IntSchTable(string initname)
        {
            name = initname;
            numRows = 0;
            columns = new Dictionary<string, IntSchColumn>();
            rows = new List<Dictionary<string, object>>();
        }

        public string Name { get => name; set => name = value; }
        
        private void printList(List<string> list, string type)
        {
            if (LOG == 1) {
                System.Console.WriteLine(type);
                list.ForEach(System.Console.WriteLine);
            }
        }

        public bool ContainsColumn(string name)
        {
            return columns.ContainsKey(name);
        }

        public List<string> getColumnNames()
        {
            return columns.Keys.ToList();
        }

        public List<Dictionary<string, object>> Select(List<string> whereClause)
        {
            if (whereClause != null && whereClause.Count > 0)
            {
                printList(whereClause, "WHERE CLAUSE");
                List<Dictionary<string, object>> filteredRows = rows.Where(row => processWhere(row, whereClause)).ToList();
                return filteredRows;
            }

            return rows;
        }

        
        // less than optimal for now, but a binary expression tree for parsing boolean logic would be good.
        private bool processWhere(Dictionary<string, object> row, List<string> whereClause)
        {
            string conditionOn = whereClause[0];
            string conditionOperator = whereClause[1];
            object condition = whereClause[2]; // this will have a type...
            object value = row[conditionOn]; // this is the stored value in the database

            TypeEnum type = columns[conditionOn].getType();

            switch (type)
            {
                case TypeEnum.String:
                    // comparisonResult is ...
                    // < 0 if value precedes condition
                    // = 0 if value same as condition
                    // > 0 if value after condition
                    int comparisonResult = ((string)value).CompareTo((string)condition);
                    switch (conditionOperator)
                    {
                        case ">": return comparisonResult < 0; // value > condition
                        case ">=": return comparisonResult <= 0; // value >= condition
                        case "<": return comparisonResult > 0; // value < condition
                        case "<=": return comparisonResult >= 0; // value <= condition
                        case "=": return comparisonResult == 0; // value == condition
                        default: throw new Exception("Unsupported condition operator");
                    }
                case TypeEnum.Integer:
                    int valueInt;
                    int conditionInt;

                    bool isValueInt = Int32.TryParse((string)value, out valueInt);
                    bool isConditionInt = Int32.TryParse((string)condition, out conditionInt);

                    if (isValueInt && isConditionInt)
                    {
                        switch (conditionOperator)
                        {
                            case ">": return valueInt > conditionInt; // value > condition
                            case ">=": return valueInt >= conditionInt; // value >= condition
                            case "<": return valueInt < conditionInt; // value < condition
                            case "<=": return valueInt <= conditionInt; // value <= condition
                            case "=": return valueInt == conditionInt; // value == condition
                            default: throw new Exception("Unsupported condition operator");
                        }
                    } else
                    {
                        throw new Exception("Condition or value wasn't a valid integer");
                    }
                case TypeEnum.Float:
                    float valueFloat;
                    float conditionFloat;

                    bool isValueFloat = Single.TryParse((string)value, out valueFloat);
                    bool isConditionFloat = Single.TryParse((string)condition, out conditionFloat);

                    if (isValueFloat && isConditionFloat)
                    {
                        switch (conditionOperator)
                        {
                            case ">": return valueFloat > conditionFloat; // value > condition
                            case ">=": return valueFloat>= conditionFloat; // value >= condition
                            case "<": return valueFloat < conditionFloat; // value < condition
                            case "<=": return valueFloat <= conditionFloat; // value <= condition
                            case "=": return valueFloat == conditionFloat; // value == condition
                            default: throw new Exception("Unsupported condition operator");
                        }
                    }
                    else
                    {
                        throw new Exception("Condition or value wasn't a valid integer");
                    }
                default:
                    return true;
            }
            
        }

        public bool Project()
        {
            throw new NotImplementedException();
        }

        // insert a record into the table
        public void Insert(List<string> columnNames, List<string> columnValues)
        {
            printList(columnNames, "Log insert column names");
            printList(columnValues, "Log insert column values");

            if (columnNames.Count == columnValues.Count)
            {
                Dictionary<string, object> row = new Dictionary<string, object>();
                
                for (int i = 0; i < columnNames.Count; ++i)
                {
                    columns[columnNames[i]].items.Add(columnValues[i]);
                    row[columnNames[i]] = columnValues[i];
                    numRows += 1;
                }

                rows.Add(row);
            }
        }

        public string AddColumn(string name, TypeEnum type)
        {
            if (columns.ContainsKey(name)) return "Column " + name + " already exists in the table" ;

            if (LOG == 1) System.Console.WriteLine("ADD COLUMN " + name + " " + type);

            columns.Add(name, new IntSchColumn(name, type));
            
            // for each row, add the column with a null value
            for (int i = 0;  i < rows.Count; ++i) rows[i][name] = null;

            return "";
        }

        // Dictionary maps column names to types
        internal string addColumns(IDictionary<string, TypeEnum> cols)
        {
            foreach (KeyValuePair<string, TypeEnum> entry in cols)
            {
                string result = AddColumn(entry.Key, entry.Value);
                if (result != "") return result;
            }

            return "Successfullly added all columns";
        }

        public string dropColumn(string columnName)
        { 
            if (columns.ContainsKey(columnName))
            {
                bool success = columns.Remove(columnName);
                if (success)
                {
                    for (int i = 0; i < rows.Count; ++i) rows[i].Remove(columnName);
                    return "Successfully removed column.";
                } else
                {
                    return "Failed to remove column.";
                }
            } else
            {
                return "Column " + columnName + " not found in the table";
            }
        }

        public List<Dictionary<string, object>> Update(Dictionary<string, string> newColValues, List<string> whereClause)
        {
            for (int i = 0; i < rows.Count; ++i)
            {
                if (whereClause != null && processWhere(rows[i], whereClause))
                {
                    updateRows(newColValues, i);
                }
                else if (whereClause == null)
                {
                    updateRows(newColValues, i);
                }
            }
                
            return Select(null);
        }

        private void updateRows(Dictionary<string, string> newColValues, int i)
        {
            foreach (KeyValuePair<string, string> columnValue in newColValues)
            {
                columns[columnValue.Key].setItemValue(columnValue.Value, i);
                rows[i][columnValue.Key] = columnValue.Value; // also update row representation... 
            }
        }

        public List<Dictionary<string, object>> DeleteRows(List<string> whereClause)
        {
            if (whereClause != null)
            {
                rows.RemoveAll(row => processWhere(row, whereClause));
                // deal with cols
            }
            else if (whereClause == null)
            {
                rows.RemoveRange(0, rows.Count);
                // deal with cols
            }

            return Select(null);
        }

        internal List<Dictionary<string, object>> OrderBy(List<Dictionary<string, object>> rows, string colToOrderOn)
        {
            // return rows.Sort(delegate(Dictionary<string, object> x, Dictionary<string, object> x {}));

            // This won't work great because typing is not considered correctly... 
            return rows.OrderBy(row => row[colToOrderOn]).ToList();
        }
    }
}
