using EduDBCore;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Forms;
using System.Linq;

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
        private List<Dictionary<string, object>> rows;

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

        public List<string> getColumnNames()
        {
            return columns.Keys.ToList();
        }

        public List<Dictionary<string, object>> Select(List<string> whereClause)
        {
            printList(whereClause, "WHERE CLAUSE");
            
            if (whereClause != null && whereClause.Count > 0)
            {
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

        public bool AddColumn(string name, TypeEnum type)
        {
            if (columns.ContainsKey(name)) return false;

            if (LOG == 1) System.Console.WriteLine("ADD COLUMN " + name + " " + type);

            columns.Add(name, new IntSchColumn(name, type));
            
            // for each row, add the column with a null value
            for (int i = 0;  i < rows.Count; ++i) rows[i][name] = null;

            return true;
        }

        // Dictionary maps column names to types
        internal bool addColumns(IDictionary<string, TypeEnum> cols)
        {
            bool result = false;
            foreach (KeyValuePair<string, TypeEnum> entry in cols)
            {
                result = AddColumn(entry.Key, entry.Value);
                if (!result) return false;
            }

            return result;
        }

        public bool dropColumn(string columnName) => columns.Remove(columnName);

        public List<Dictionary<string, object>> Update(Dictionary<string, string> newColValues, List<string> whereClause)
        {
            for (int i = 0; i < rows.Count; ++i)
            {
                if (processWhere(rows[i], whereClause))
                {
                    foreach (KeyValuePair<string, string> columnValue in newColValues)
                    {
                        columns[columnValue.Key].setItemValue(columnValue.Value, i);
                        rows[i][columnValue.Key] = columnValue.Value; // also update row representation... 
                    }
                }
            }
                
            return Select(null);
        }
    }
}
