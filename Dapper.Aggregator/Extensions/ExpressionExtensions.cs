using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Dapper.Aggregator
{
    public static class ExpressionExtensions
    {
        public static ColumnAttribute ToColumnInfo(this Expression expr)
        {
            var lambdaExp = (LambdaExpression)expr;
            var memExp = lambdaExp.Body as MemberExpression;
            if (memExp == null)
            {
                var convert = lambdaExp.Body as UnaryExpression;
                if (convert != null)
                {
                    memExp = convert.Operand as MemberExpression;
                }
            }
            return memExp.Member.CreateColumnInfo();
        }
    }
}
