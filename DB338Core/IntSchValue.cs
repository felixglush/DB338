using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using System.Threading.Tasks;

namespace EduDBCore
{

    class IntSchValue : IComparable
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

        // if that > that, return 1
        // if this == that, return 0
        // if this < that, return -1
        public int CompareTo(object that)
        {
            IntSchValue thatVal = (IntSchValue)that;
            TypeEnum thatType = thatVal.Type;

            if (thatType == this.Type)
            {
                switch(this.Type)
                {
                    case TypeEnum.Integer:
                        int thisInt;
                        int thatInt;

                        bool isThisInt = Int32.TryParse((string)this.Value, out thisInt);
                        bool isThatInt = Int32.TryParse((string)thatVal.Value, out thatInt);

                        if (isThisInt && isThatInt)
                        {
                            if (thisInt < thatInt) return -1;
                            else if (thisInt == thatInt) return 0;
                            else return 1;
                        }
                        break;
                    case TypeEnum.Float:
                        float thisFloat;
                        float thatFloat;

                        bool isThisFloat = Single.TryParse((string)this.Value, out thisFloat);
                        bool isThatFloat = Single.TryParse((string)thatVal.Value, out thatFloat);

                        if (isThisFloat && isThatFloat)
                        {
                            if (thisFloat < thatFloat) return -1;
                            else if (thisFloat == thatFloat) return 0;
                            else return 1;
                        }
                        break;
                    case TypeEnum.String:
                        return String.Compare((string)Value, (string)thatVal.Value);
                    default:
                        return 0;
                }
            }

            return 0;
        }
    }
}
