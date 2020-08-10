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

        public void setItemValue(string value, int index)
        {
            bool possible = false; // possible if the given type is casteable to the column's data type
            switch (dataType)
            {
                case TypeEnum.Integer:
                    int resultInt;
                    possible = Int32.TryParse(value, out resultInt);
                    break;
                case TypeEnum.Float:
                    float resultFloat;
                    possible = Single.TryParse(value, out resultFloat);
                    break;
                default:
                    break;
            }

            if (!possible)
            {
                throw new Exception("Value type is not the correct for this column. Column: " + name + " has type " + dataType);
            }

            items[index] = value;
        }

        public TypeEnum getType()
        {
            return dataType;
        }

        public string Name { get => name; set => name = value; }
    }
}
