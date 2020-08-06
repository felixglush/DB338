using System;
using System.Collections.Generic;
using System.Text;

namespace DB338Core
{
    class IntSchColumn
    {
        public List<string> items;
        private string dataType;
        private string name;

        public IntSchColumn(string newname, string type)
        {
            name = newname;
            dataType = type;
            items = new List<string>();
        }
        public string Get(int pos)
        {
            return items[pos];
        }

        public string Name { get => name; set => name = value; }
    }
}
