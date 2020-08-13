using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EduDBCore
{
    class IntSchValueEqualityByValue : IEqualityComparer<IntSchValue>
    {
        public bool Equals(IntSchValue x, IntSchValue y)
        {
            if (x.Type != y.Type) return false;
            return x.CompareTo(y) == 0;
        }

        public int GetHashCode(IntSchValue obj)
        {
            return obj.Value.GetHashCode();
        }
    }
}
