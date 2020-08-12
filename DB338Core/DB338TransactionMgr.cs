using EduDBCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace DB338Core
{
    class DB338TransactionMgr
    {
        //the List of Internal Schema Tables holds the actual data for DB338
        //it is implemented using Lists, which could be replaced.
        // replaced with a dictionary of table name to IntSchTable
        IDictionary<string, IntSchTable> tables;

        public DB338TransactionMgr()
        {
            tables = new Dictionary<string, IntSchTable>();
        }

        public string[,] Process(List<string> tokens, string type)
        {
            Console.WriteLine("Tokens:");
            foreach(string t in tokens)
            {    
                Console.WriteLine(t);
            }

            string[,] results;
            if (type == "create")
            {
                results = ProcessCreateTableStatement(tokens);
            }
            else if (type == "insert")
            {
                results = ProcessInsertStatement(tokens);
            }
            else if (type == "select")
            {
                results = ProcessSelectStatement(tokens);
            }
            else if (type == "alter")
            {
                results = ProcessAlterStatement(tokens);
            }
            else if (type == "delete")
            {
                results = ProcessDeleteStatement(tokens);
            }
            else if (type == "drop")
            {
                results = ProcessDropStatement(tokens);
            }
            else if (type == "update")
            {
                results = ProcessUpdateStatement(tokens);
            }
            else
            {
                results = null;
            }
            //other parts of SQL to do...

            return results;
        }

        private static string[,] ConvertToArray(List<IntSchRow> rows, List<string> columnNames)
        {
            string[,] returnResult = new string[rows.Count, columnNames.Count];

            // for each row
            for (int i = 0; i < rows.Count; ++i)
            {
                // for each column
                for (int j = 0; j < columnNames.Count; ++j)
                {
                    string str = rows[i].GetValueInColumn(columnNames[j]).Value as string;
                    if (str != null)
                    {
                        returnResult[i, j] = str;
                    } else
                    {
                        throw new InvalidCastException("Value not convertible to string");
                    }
                }
            }

            return returnResult;
        }

        // create table test(col1 whatever, col2 whatever, col3 whatever)
        private string[,] ProcessCreateTableStatement(List<string> tokens)
        {
            // assuming only the following rule is accepted
            // <Create Stm> ::= CREATE TABLE Id '(' <ID List> ')'  ------ NO SUPPORT for <Constraint Opt>

            string newTableName = tokens[2];

            if (tables.ContainsKey(newTableName))
            {
                //cannot create a new table with the same name
                return new string[,] { { "Table " + newTableName +  " already exists"} };
            }

            List<string> columnNames = new List<string>();
            List<TypeEnum> columnTypes = new List<TypeEnum>();

            int idCount = 2;
            for (int i = 4; i < tokens.Count; ++i)
            {
                if (tokens[i] == ")")
                {
                    break;
                }
                else if (tokens[i] == ",")
                {
                    continue;
                }
                else
                {
                    if (idCount == 2)
                    {
                        columnNames.Add(tokens[i]);
                        --idCount;
                    }
                    else if (idCount == 1)
                    {
                        TypeEnum type;

                        switch (tokens[i])
                        {
                            case "varchar":
                                type = TypeEnum.String;
                                break;
                            case "int":
                                type = TypeEnum.Integer;
                                break;
                            case "float":
                                type = TypeEnum.Float;
                                break;
                            default:
                                return new string[,] { { "Type is not supported: " + tokens[i] } };
                        }

                        columnTypes.Add(type);
                        idCount = 2;
                    }
                }
            }

            IntSchTable newTable = new IntSchTable(newTableName);

            for (int i = 0; i < columnNames.Count; ++i)
            {
                newTable.AddColumn(columnNames[i], columnTypes[i]);
            }

            tables.Add(newTableName, newTable);

            return new string[,] { { "Succesfully created table "  + newTableName } };
        }

        // insert into test(col1, col2, col3) values(100, 200, 300)
        // insert into test(col1, col2, col3) values(1000, 2000, 3000)
        // insert into test(col1, col2, col3) values(10000, 20000, 30000)    
        private string[,] ProcessInsertStatement(List<string> tokens)
        {
            // <Insert Stm> ::= INSERT INTO Id '(' <ID List> ')' VALUES '(' <Expr List> ')'

            List<string> columnNames = new List<string>();
            List<string> columnValues = new List<string>();

            int firstValueOffset = 0;

            for (int i = 4; i < tokens.Count; ++i)
            {
                if (tokens[i] == ")")
                {
                    firstValueOffset = i + 3;
                    break;
                }
                else if (tokens[i] == ",")
                {
                    continue;
                }
                else
                {
                    columnNames.Add(tokens[i]);
                }
            }

            for (int i = firstValueOffset; i < tokens.Count; ++i)
            {
                if (tokens[i] == ")")
                {
                    break;
                }
                else if (tokens[i] == ",")
                {
                    continue;
                }
                else
                {
                    columnValues.Add(tokens[i]);
                }
            }

            if (columnNames.Count != columnValues.Count)
            {
                return new string[,] { { "Column count doesn't match value count" } };
            }
            else
            {
                string insertTableName = tokens[2];
                tables[insertTableName].Insert(columnNames, columnValues);
                return new string[,] { { "Table modified" } };
            }
        }

        // select col1, col2, col3 from test
        private string[,] ProcessSelectStatement(List<string> tokens)
        {
            // <Select Stm> ::= SELECT <Columns> <From Clause> <Where Clause> <Group Clause> <Having Clause> <Order Clause>

            int indexWhere = tokens.IndexOf("where");
            int indexGroupby = tokens.IndexOf("group");
            int indexHaving = tokens.IndexOf("having");
            int indexOrderby = tokens.IndexOf("order");

            if (indexGroupby == -1 && indexHaving != -1)
            {
                // return error, must have group by
                return new string[,] { { "Error: Having clause must have group by." } };
            }

            List<string> colsToSelect = new List<string>();

            int indexOfFrom = tokens.IndexOf("from");
            int tableOffset = 0;

            for (int i = 1; i < tokens.Count; ++i)
            {
                if (tokens[i] == "from")
                {
                    tableOffset = i + 1;
                    break;
                }
                else if (tokens[i] == ",")
                {
                    continue;
                }
                else
                {
                    colsToSelect.Add(tokens[i]); // TODO: aggregate function detection...
                }
            }

            List<string> whereClause = new List<string>();

            if (indexWhere != -1)
            {
                // process where clause
                int i = indexWhere + 1;
                while (i < tokens.Count)
                {
                    whereClause.Add(tokens[i]);
                    if (i == indexGroupby || i == indexOrderby)
                    {
                        break;
                    }
                    i += 1;
                }
            }

            string tableToSelectFrom = tokens[tableOffset];

            if (tables.ContainsKey(tableToSelectFrom))
            {
                // list of rows is returned. each row is a mapping between the column name and its value in that row.
                List<IntSchRow> result = tables[tableToSelectFrom].Select(whereClause); // Select will check if where is empty or not

                if (indexGroupby != -1)
                {

                    // process group by clause
                    if (indexHaving != -1)
                    {
                        // process having clause 
                        // not implemented yet
                    }

                    string colToGroupOn = tokens[indexGroupby + 2];
                    //IEnumerable<IGrouping<object, Dictionary<string, object>>> query = result.GroupBy(record => record[colToGroupOn]);

                }

                if (indexOrderby != -1)
                {
                    // process order by clause
                    string colToOrderOn = tokens[indexOrderby + 2];
                    result = tables[tableToSelectFrom].OrderBy(result, colToOrderOn);
                }

                if (colsToSelect.Count == 1 && colsToSelect[0] == "*")
                {
                    colsToSelect.RemoveAt(0);
                    colsToSelect = tables[tableToSelectFrom].getColumnNames();
                }

                string[,] returnResult = new string[result.Count, colsToSelect.Count];
                
                // for each row
                for (int i = 0; i < result.Count; ++i)
                {
                    // for each column
                    for (int j = 0; j < colsToSelect.Count; ++j)
                    {
                        if (tables[tableToSelectFrom].ContainsColumn(colsToSelect[j]))
                        {
                            string str = result[i].GetValueInColumn(colsToSelect[j]).Value as string;
                            if (str != null)
                            {
                                returnResult[i, j] = str;
                            } else
                            {
                                throw new InvalidCastException("Result not convertible to string");
                            }
                        } else
                        {
                            return new string[,] { { "No such column " + colsToSelect[j] + " in the table " + tableToSelectFrom } };
                        }
                    }
                }

                return returnResult;
            }
            else
            {
                return new string[,] { { "No such table in the database" } };
            }   
        }

        private string[,] ProcessUpdateStatement(List<string> tokens)
        {
            // <Update Stm> ::= UPDATE Id SET <Assign List> <Where Clause>
            /** example:
             * update customers
             * set contactname = "alfred", city = "toronto"
             * where id = 1
             * 
             * if no where, update every row
             */

            string tableName = tokens[1];
            Dictionary<string, string> newColValues = new Dictionary<string, string>();
            List<string> whereClause = null;

            int endAssign = tokens.IndexOf("where");

            if (endAssign != -1)
            {
                whereClause = new List<string>();
                for (int i = endAssign + 1; i < tokens.Count; ++i)
                {
                    whereClause.Add(tokens[i]);
                }
            }
            else
            {
                endAssign = tokens.Count;
            }

            for (int i = 3; i < endAssign; ++i)
            {
                if (tokens[i] == "=")
                {
                    newColValues[tokens[i - 1]] = tokens[i + 1];
                }
            }

            string[,] result = tables[tableName].Update(newColValues, whereClause);
            return result;
        }
       
        private string[,] ProcessDropStatement(List<string> tokens)
        {
            // <Drop Stm> ::= DROP TABLE Id
            string tableName = tokens[2];
            bool success = tables.Remove(tableName);

            if (success)
            {
                return new string[,] { { "Success: Table " + tableName + " removed." } };
            } else
            {
                return new string[,] { { "Fail: No such table found." } };
            }
        }

        private string[,] ProcessDeleteStatement(List<string> tokens)
        {
            // <Delete Stm> ::= DELETE FROM Id <Where Clause>

            string tableName = tokens[2];
            if (tables.ContainsKey(tableName))
            {
                List<string> columnNames = tables[tableName].getColumnNames();
                List<IntSchRow> result;

                int indexWhere = tokens.IndexOf("where");

                if (indexWhere == -1) // deletes every row
                {
                    result = tables[tableName].DeleteRows(null);
                    return ConvertToArray(result, columnNames);
                }
                else
                {
                    List<string> whereClause = new List<string>();
                    for (int i = indexWhere + 1; i < tokens.Count; ++i)
                    {
                        whereClause.Add(tokens[i]);
                    }

                    result = tables[tableName].DeleteRows(whereClause);
                    return ConvertToArray(result, columnNames);
                }
            } else
            {
                return new string[,] { { "No such table in the database" } };
            }
        }

        private string[,] ProcessAlterStatement(List<string> tokens)
        {
            // <Alter Stm> ::= ALTER TABLE Id ADD COLUMN <Field Def List> <Constraint Opt>
            // <Alter Stm> ::= ALTER TABLE Id ADD <Constraint>
            // <Alter Stm> ::= ALTER TABLE Id DROP COLUMN Id
            // <Alter Stm> ::= ALTER TABLE Id DROP CONSTRAINT Id

            // <Field Def> ::= Id <Type> NOT NULL
            // <Field Def> ::= Id <Type>
            // <Field Def List> ::= <Field Def> ',' <Field Def List>
            // <Field Def List> ::= <Field Def>--

            string tableName = tokens[2];
            string action = tokens[3]; // add or drop
            string what = tokens[4]; // column or constraint

            if (tables.ContainsKey(tableName))
            {
                if (action == "add")
                {
                    if (what == "column")
                    {
                        // map column name to type
                        IDictionary<string, TypeEnum> columns = new Dictionary<string, TypeEnum>();

                        int i = 6;
                        int idCount = 2;
                        while (i < tokens.Count || tokens[i] != "constraint")
                        {
                            if (tokens[i] != ",")
                            {
                                string name = "";
                                string type = ""; // default

                                if (idCount == 2) // column name
                                {
                                    name = tokens[i];
                                    --idCount;
                                }
                                else if (idCount == 1) // column type
                                {
                                    type = tokens[i];
                                    idCount = 2;
                                }

                                switch (type)
                                {
                                    case "varchar":
                                        columns.Add(name, TypeEnum.String);
                                        break;
                                    case "int":
                                        columns.Add(name, TypeEnum.Integer);
                                        break;
                                    case "float":
                                        columns.Add(name, TypeEnum.Float);
                                        break;
                                    default:
                                        throw new Exception("Unsupported type: " + type);
                                }
                            }
                            i += 1;
                        }

                        string result = tables[tableName].AddColumns(columns);
                        return new string[,] { { result } };
                    }
                    else if (what == "constraint")
                    {
                        throw new NotImplementedException();
                    }
                }
                // <Alter Stm> ::= ALTER TABLE Id DROP COLUMN Id
                // <Alter Stm> ::= ALTER TABLE Id DROP CONSTRAINT Id
                else if (action == "drop")
                {
                    if (what == "column")
                    {
                        string columnName = tokens[5];
                        string result = tables[tableName].dropColumn(columnName);
                        return new string[,] { { result } };
                    }
                    else if (what == "constraint")
                    {
                        throw new NotImplementedException();
                    }
                }

                return new string[,] { { "Error" } }; ;
            }
            else
            {
                return new string[,] { { "No such table in database" } };
            }
        }
    }
}
