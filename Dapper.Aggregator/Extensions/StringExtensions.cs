using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    public static class StringExtensions
    {
        public static string EscapeAliasFormat(this string s)
        {
            s = s.Replace("\"", "\"\"");
            return $"\"{s}\"";
        }
    }
}
