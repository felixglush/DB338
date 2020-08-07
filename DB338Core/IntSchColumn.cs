using EduDBCore;
using System;
using System.Collections.Generic;
using System.Text;

namespace DB338Core
{
    class IntSchColumn
    {
        public List<string> items;
        private TypeEnum dataType;
        private string name;

        public IntSchColumn(string newname, TypeEnum type)
        {
            name = newname;
            dataType = type;
            items = new List<string>();
        }
        public string Get(int pos)
        {
            return items[pos];
        }

        public TypeEnum getType()
        {
            return dataType;
        }

        public string Name { get => name; set => name = value; }
    }
}
