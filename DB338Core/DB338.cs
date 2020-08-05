using System;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Windows.Forms.VisualStyles;

namespace DB338Core
{
    public class DB338
    {
        private SQLParser sqlParser;
        private DB338TransactionMgr transactionMgr;

        public DB338()
        {
            sqlParser = new SQLParser();
            sqlParser.Setup();
            transactionMgr = new DB338TransactionMgr();
        }

        public QueryResult SubmitQuery(string query)
        {
            TextReader scriptText = new StringReader(query);

            List<string> tokens = sqlParser.Parse(scriptText);

            //sqlParser.Parse will always return the tokens of the query if accepted
            //the TYPE of the query is passed into the transaction manager, so you know what to check

            string queryType = tokens[tokens.Count - 4];
            string done = tokens[tokens.Count - 3];
            string accepted = tokens[tokens.Count - 2];
            string error = tokens[tokens.Count - 1];

            tokens.RemoveAt(tokens.Count - 1);
            tokens.RemoveAt(tokens.Count - 1);
            tokens.RemoveAt(tokens.Count - 1);
            tokens.RemoveAt(tokens.Count - 1);

            if (accepted == "True")
            {
                string[,] results = transactionMgr.Process(tokens, queryType);
                QueryResult queryResult = new QueryResult(queryType, done, accepted, error);
                queryResult.Results = results;
                return queryResult;
            }
            else
            {
                //error
                QueryResult queryResult = new QueryResult(queryType, done, accepted, error);
                queryResult.Results = null;
                return queryResult;
            }
        }

    }
}
