using EduDBCore;
using GOLD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
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
            string[,] results = new string[1,1];
            bool success;

            Console.WriteLine("Tokens:");
            foreach(string t in tokens)
            {    
                Console.WriteLine(t);
            }

            if (type == "create")
            {
                success = ProcessCreateTableStatement(tokens);
            }
            else if (type == "insert")
            {
                success = ProcessInsertStatement(tokens);
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
        
        // create table test(col1 whatever, col2 whatever, col3 whatever)
        private bool ProcessCreateTableStatement(List<string> tokens)
        {
            // assuming only the following rule is accepted
            // <Create Stm> ::= CREATE TABLE Id '(' <ID List> ')'  ------ NO SUPPORT for <Constraint Opt>

            string newTableName = tokens[2];

            if (tables.ContainsKey(newTableName))
            {
                //cannot create a new table with the same name
                return false;
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
                                throw new Exception("Type not supported: " + tokens[i]);
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

            return true;
        }

        // insert into test(col1, col2, col3) values(100, 200, 300)
        // insert into test(col1, col2, col3) values(1000, 2000, 3000)
        // insert into test(col1, col2, col3) values(10000, 20000, 30000)    
        private bool ProcessInsertStatement(List<string> tokens)
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
                return false;
            }
            else
            {
                string insertTableName = tokens[2];
                tables[insertTableName].Insert(columnNames, columnValues);
                return true;
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
                return new string[1, 1] { { "Error: Having clause must have group by." } };
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

            // list of rows is returned. each row is a mapping between the column name and its value in that row.
            List<Dictionary<string, object>> result = tables[tableToSelectFrom].Select(whereClause); // Select will check if where is empty or not

            if (indexGroupby != -1)
            {

                // process group by clause
                if (indexHaving != -1)
                {
                    // process having clause 
                    // not implemented yet
                }

                string colToGroupOn = tokens[indexGroupby + 1];
                //IEnumerable<IGrouping<object, Dictionary<string, object>>> query = result.GroupBy(record => record[colToGroupOn]);

            }

            if (indexOrderby != -1)
            {
                // process order by clause
                string colToOrderOn = tokens[indexOrderby + 1];

                IEnumerable<Dictionary<string, object>> query = result.OrderBy(record => record[colToOrderOn]);

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
                    returnResult[i, j] = (string)result[i][colsToSelect[j]];   
                }
            }

            return returnResult;
        }

        private string[,] ProcessUpdateStatement(List<string> tokens)
        {
            // <Update Stm> ::= UPDATE Id SET <Assign List> <Where Clause>
            /** example:
             * update customers
             * set contactname = "alfred", city = "toronto"
             * where id = 1
             */

            string tableName = tokens[1];
            Dictionary<string, string> newColValues = new Dictionary<string, string>();
            List<string> whereClause = new List<string>();

            int whereOffset = tokens.IndexOf("where");

            if (whereOffset != -1)
            {
                for (int i = whereOffset + 1;  i < tokens.Count; ++i)
                {
                    whereClause.Add(tokens[i]);
                }
            }

            for (int i = 3; i < whereOffset;  ++i)
            {
                if (tokens[i] == "=")
                {
                    newColValues[tokens[i - 1]] = tokens[i + 1];
                }
            }

            // list of rows is returned. each row is a mapping between the column name and its value in that row.
            List<Dictionary<string, object>> result = tables[tableName].Update(newColValues, whereClause);
            List<string> columns = result[0].Keys.ToList();
            string[,] returnResult = new string[result.Count, columns.Count];
            
            // for each row
            for (int i = 0; i < result.Count; ++i)
            {
                // for each column
                for (int j = 0; j < result[0].Count; ++j)
                {
                    returnResult[i, j] = (string)result[i][columns[j]];
                }
            }

            return returnResult;
        }

        private string[,] ProcessDropStatement(List<string> tokens)
        {
            // <Drop Stm> ::= DROP TABLE Id
            string[,] result = new string[1, 1];
            string tableName = tokens[2];
            
            bool success = tables.Remove(tableName);

            if (success)
            {
                result[0, 0] = "Success: Table " + tableName + " removed.";
            } else
            {
                result[0, 0] = "Fail: No such table found.";
            }

            return result;
        }

        private string[,] ProcessDeleteStatement(List<string> tokens)
        {
            // <Delete Stm> ::= DELETE FROM Id <Where Clause>
            string tableName = tokens[3];

            // some kind of boolean logic to process the where condition

            throw new NotImplementedException();
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

            // constraints to implement?

            string tableName = tokens[2];
            string action = tokens[3];
            string what = tokens[4];
            bool result = false;

            if (action == "add")
            {
                
                if (what == "column")
                {
                    // map column name to "type" and "not null"
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
                                // TODO: handle "NOT NULL" (1 token or 2?)
                                type = tokens[i];
                                idCount = 2;
                            }

                            switch (type)
                            {
                                case "string":
                                    columns.Add(name, TypeEnum.String);
                                    break;
                                case "integer":
                                    columns.Add(name, TypeEnum.Integer);
                                    break;
                                case "float":
                                    columns.Add(name, TypeEnum.Float);
                                    break;
                                default:
                                    throw new Exception("Unsuported type: " + type);
                            }
                        }
                    }

                    bool success = tables[tableName].addColumns(columns);
                    if (success)
                    {
                        return new string[,] { { "Add columns successful" } };
                    }
                    else
                    {
                        return new string[,] { { "Error: add columns unsuccessful" } };
                    }
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
                    bool success = tables[tableName].dropColumn(columnName);
                    if (success)
                    {
                        return new string[,] { { "Drop successful" } };
                    } else
                    {
                        return new string[,] { { "Error: drop unsuccessful" } };
                    }
                } else if (what == "constraint")
                {
                    throw new NotImplementedException();
                }
            }

            return new string[,] { { "Error" } }; ;
        }
    }
}
