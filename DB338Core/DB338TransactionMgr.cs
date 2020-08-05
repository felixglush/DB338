using GOLD;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.AccessControl;
using System.Text;
using System.Threading.Tasks;

namespace DB338Core
{
    class DB338TransactionMgr
    {
        List<IntSchTable> tables;

        public DB338TransactionMgr()
        {
            tables = new List<IntSchTable>();
        }

        public string[,] Process(List<string> tokens, string type)
        {
            string[,] results = new string[1,1];
            bool success;

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

        private string[,] ProcessSelectStatement(List<string> tokens)
        {
            // <Select Stm> ::= SELECT <Columns> <From Clause> <Where Clause> <Group Clause> <Having Clause> <Order Clause>

            List<string> colsToSelect = new List<string>();
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
                    colsToSelect.Add(tokens[i]);
                }
            }

            string tableToSelectFrom = tokens[tableOffset];

            for (int i = 0; i < tables.Count; ++i)
            {
                if (tables[i].Name == tableToSelectFrom)
                {
                    return tables[i].Select(colsToSelect);
                }
            }

            return null;
        }

        private bool ProcessInsertStatement(List<string> tokens)
        {
            // <Insert Stm> ::= INSERT INTO Id '(' <ID List> ')' VALUES '(' <Expr List> ')'

            string insertTableName = tokens[2];

            foreach (IntSchTable tbl in tables)
            {
                if (tbl.Name == insertTableName)
                {
                    List<string> columnNames = new List<string>();
                    List<string> columnValues = new List<string>();

                    int offset = 0;

                    for (int i = 4; i < tokens.Count; ++i)
                    {
                        if (tokens[i] == ")")
                        {
                            offset = i + 3;
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

                    for (int i = offset; i < tokens.Count; ++i)
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
                        tbl.Insert(columnNames, columnValues);
                        return true;
                    }
                }
            }

            return false;
        }

        private bool ProcessCreateTableStatement(List<string> tokens)
        {
            // assuming only the following rule is accepted
            // <Create Stm> ::= CREATE TABLE Id '(' <ID List> ')'  ------ NO SUPPORT for <Constraint Opt>

            string newTableName = tokens[2];

            foreach (IntSchTable tbl in tables)
            {
                if (tbl.Name == newTableName)
                {
                    //cannot create a new table with the same name
                    return false;
                }
            }

            List<string> columnNames = new List<string>();
            List<string> columnTypes = new List<string>();

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
                        columnTypes.Add(tokens[i]);
                        idCount = 2;
                    }
                }
            }

            IntSchTable newTable = new IntSchTable(newTableName);

            for (int i = 0; i < columnNames.Count; ++i)
            {
                newTable.AddColumn(columnNames[i], columnTypes[i]);
            }

            tables.Add(newTable);

            return true;
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
            List<string> cols = new List<string>();
            List<string> newVals = new List<string>();
            string conditionCol = null;
            string conditionVal = null;
            string condition = null;

            int whereOffset = tokens.IndexOf("where");
            int assignEnd = tokens.Count;

            if (whereOffset != -1)
            {
                assignEnd = whereOffset;

                // where col = cond
                conditionCol = tokens[whereOffset + 1];
                condition = tokens[whereOffset + 2];
                conditionVal = tokens[whereOffset + 3];
            }

            for (int i = 3; i < assignEnd;  ++i)
            {
                if (tokens[i] == "=")
                {
                    cols.Add(tokens[i -  1]);
                    newVals.Add(tokens[i + 1]);
                }
            }

            for (int i = 0; i < tables.Count; ++i)
            {
                if (tables[i].Name == tableName)
                {
                    return tables[i].Update(cols, newVals, conditionCol, conditionVal, condition);
                }
            }

            return null;
        }

        private string[,] ProcessDropStatement(List<string> tokens)
        {
            // <Drop Stm> ::= DROP TABLE Id
            string[,] result = new string[1, 1];
            string tableName = tokens[2];
            
            result[0, 0] = "Fail: No such table found.";

            for (int i = 0; i < tables.Count; ++i)
            {
                if (tables[i].Name == tableName)
                {
                    tables.RemoveAt(i);
                    result[0, 0] = "Success: Table " + tableName +  " removed.";
                }
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

            string tableName = tokens[3];
            string action = tokens[4];
            string what = tokens[5];

            if (action == "add")
            {
                if (what == "column")
                {

                }
                else if (what == "constraint")
                {
                    throw new NotImplementedException();
                }
            } 
            else if (action == "drop")
            {
                if (what == "column")
                {

                } else if (what == "constraint")
                {
                    throw new NotImplementedException();
                }
            }

        }
    }
}
