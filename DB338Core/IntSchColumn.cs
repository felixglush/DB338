using EduDBCore;
using System;
using System.Collections.Generic;
using System.Text;

namespace DB338Core
{
    class IntSchColumn
    {
        private List<IntSchValue> items;
        private TypeEnum dataType;
        private string name;

        public IntSchColumn(string newname, TypeEnum type)
        {
            name = newname;
            DataType = type;
            items = new List<IntSchValue>();
        }

        public void AddValueToColumn(IntSchValue value)
        {
            items.Add(value);
        }

        public void RemoveValueFromColumn(int index)
        {
            items.RemoveAt(index);
        }

        public string Name { get => name; set => name = value; }
        internal TypeEnum DataType { get => dataType; set => dataType = value; }
    }
}
