using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EduDBCore
{

    class IntSchValue
    {
        private object _value;

        public IntSchValue(object value, TypeEnum type)
        {
            this.Value = value;
            this.Type = type;
        }

        public object Value
        {
            get
            {
                return _value;
            }
            set
            {
                /**
                bool possible = false; // possible if the given type is casteable to the column's data type
                switch (this.Type)
                {
                    case TypeEnum.Integer:
                        int resultInt;
                        possible = Int32.TryParse((string)value, out resultInt);
                        break;
                    case TypeEnum.Float:
                        float resultFloat;
                        possible = Single.TryParse((string)value, out resultFloat);
                        break;
                    case TypeEnum.String:
                        var str = value as string;
                        if (str != null) possible = true;
                        break;
                    default:
                        return false;
                }
                */
                this._value = value;
            }
        }

        internal TypeEnum Type { get; set; }
    }
}
