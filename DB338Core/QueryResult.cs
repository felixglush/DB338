using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DB338Core
{
    public struct QueryResult
    {
        private string queryType;
        private string done;
        private string accepted;
        private string error;

        private string[,] results;

        public QueryResult(string qt, string d, string acc, string err)
        {
            queryType = qt;
            done = d;
            accepted = acc;
            error = err;
            results = null;
        }

        public string[,] Results { get => results; set => results = value; }
        public string QueryType { get => queryType; set => queryType = value; }
        public string Done { get => done; set => done = value; }
        public string Accepted { get => accepted; set => accepted = value; }
        public string Error { get => error; set => error = value; }
    }
}
