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
        private IDictionary<string, IntSchColumn> columns;

        // list of records, each record is a map between column name and the value. this makes filtering out entire rows easier.
        // otherwise, would have to create a list of indices for which rows to filter out when going through the columns variable
        private List<IntSchRow> rows;

        private int LOG = 1; // maybe make a logger class?

        public IntSchTable(string initname)
        {
            Name = initname;
            columns = new Dictionary<string, IntSchColumn>();
            rows = new List<IntSchRow>();
        }

        public string Name { get; set; }

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

        public List<IntSchRow> Select(List<string> whereClause)
        {
            if (whereClause != null && whereClause.Count > 0)
            {
                printList(whereClause, "WHERE CLAUSE");
                List<IntSchRow> filteredRows = rows.Where(row => processWhere(row, whereClause)).ToList();
                return filteredRows;
            }

            return rows;
        }

        
        // less than optimal for now, but a binary expression tree for parsing boolean logic would be good.
        private bool processWhere(IntSchRow row, List<string> whereClause)
        {
            string conditionOn = whereClause[0];
            string conditionOperator = whereClause[1];
            object condition = whereClause[2]; // this will have a type...
            object value = row.GetValueInColumn(conditionOn).Value; // this is the stored value in the database

            TypeEnum type = columns[conditionOn].DataType;

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
                IntSchRow row = new IntSchRow();
                
                for (int i = 0; i < columnNames.Count; ++i)
                {
                    TypeEnum columnType = columns[columnNames[i]].DataType;
                    IntSchValue val = new IntSchValue(columnValues[i], columnType);

                    columns[columnNames[i]].AddValueToColumn(val);
                    row.SetValueInColumn(columnNames[i], val);
                }

                rows.Add(row);
            }
        }

        public string AddColumn(string name, TypeEnum type)
        {
            if (columns.ContainsKey(name)) return "Column " + name + " already exists in the table" ;

            if (LOG == 1) System.Console.WriteLine("ADD COLUMN " + name + " " + type);

            // make a rows.Count quantity of new IntSchValues and add them to the column and each row
            IntSchColumn newCol = new IntSchColumn(name, type);
            for (int i = 0; i < rows.Count; ++i)
            {
                IntSchValue val = new IntSchValue(null, type);
                rows[i].SetValueInColumn(name, val);
                newCol.AddValueToColumn(val);
            }
            columns.Add(name, newCol);

            return "";
        }

        // Dictionary maps column names to types
        internal string AddColumns(IDictionary<string, TypeEnum> cols)
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
                    for (int i = 0; i < rows.Count; ++i) rows[i].RemoveColumn(columnName);
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

        public string[,] Update(Dictionary<string, string> newColValues, List<string> whereClause)
        {
            for (int i = 0; i < rows.Count; ++i)
            {
                bool result = false;
                if (whereClause != null && processWhere(rows[i], whereClause))
                {
                    result = updateRows(newColValues, i);
                }
                else if (whereClause == null)
                {
                    result = updateRows(newColValues, i); // every single row i gets passed in here
                }

                if (!result)
                {
                    return new string[,] { { "Rows couldn't be updated. Given column type likely doesn't match the column's stored type." } };
                }
            }
                
            return new string[,] { { "Rows updated succesfully" } };
        }

        private bool updateRows(Dictionary<string, string> newColValues, int rowIndex)
        {
            foreach (KeyValuePair<string, string> columnValue in newColValues)
            {
                // get the IntSchValue by rowIndex and update it with newColValues.
                bool result = rows[rowIndex].UpdateValueInColumm(columnValue.Key, columnValue.Value);
                if (!result)
                {
                    return result;
                }
            }
            return true;
        }

        // fix
        public List<IntSchRow> DeleteRows(List<string> whereClause)
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

        internal List<IntSchRow> OrderBy(List<IntSchRow> rows, string colToOrderOn)
        {
            // return rows.Sort(delegate(Dictionary<string, object> x, Dictionary<string, object> x {}));

            // This won't work great because typing is not considered correctly... 
            return rows.OrderBy(row => row.GetValueInColumn(colToOrderOn)).ToList();
        }
    }
}
